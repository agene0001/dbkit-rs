//! Native-Postgres handler — sqlx [`PgPool`] with full Postgres type support.
//!
//! Mirrors [`BaseHandler`](crate::BaseHandler)'s write surface, but binds the
//! *rich* [`DbValue`] variants (date / timestamp / json / uuid) to their native
//! Postgres types via sqlx, and returns native [`PgRow`](sqlx::postgres::PgRow)s.
//! Use this when you need Postgres types the multi-backend `Any` pool can't
//! represent.
//!
//! Reads use the ergonomic row-mapped [`ReadOp`] API over DuckDB (typically
//! attached live to Postgres via [`with_duckdb_attached_postgres`]).
//!
//! [`with_duckdb_attached_postgres`]: PgHandler::with_duckdb_attached_postgres

use crate::DbkitError;
use crate::base_handler::{FetchMode, QueryResult, WriteOp};
use crate::value::DbValue;
use std::fmt::Write as _;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::{AssertSqlSafe, PgPool, Postgres};
use tracing::warn;
use unicode_normalization::UnicodeNormalization;

#[cfg(feature = "duckdb")]
use crate::analytical::RecordBatch;
#[cfg(feature = "duckdb")]
use crate::read::{ReadEngine, duckdb::DuckEngine};

/// A typeless SQL `NULL`. Declares the Postgres parameter type as OID 0 so the
/// server infers it from context — exactly like a bare `NULL` literal. This lets
/// a `NULL` [`DbValue`] unify with any column type in `COALESCE` / `CASE` / etc.,
/// instead of being pinned to one concrete type. (Binding `Option::<i64>::None`
/// forced `int8`, which broke e.g. `COALESCE($1, external_id)` against a
/// `varchar` column: "bigint and character varying cannot be matched".)
struct PgNull;

impl sqlx::Type<Postgres> for PgNull {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        // OID 0 → "unspecified", resolved from context by the server.
        sqlx::postgres::PgTypeInfo::with_oid(sqlx::postgres::types::Oid(0))
    }
}

impl<'q> sqlx::Encode<'q, Postgres> for PgNull {
    fn encode_by_ref(
        &self,
        _buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        Ok(sqlx::encode::IsNull::Yes)
    }
}

/// Bind a slice of [`DbValue`]s onto a sqlx Postgres query, in order, binding
/// the rich variants to their native Postgres types (no text fallback).
/// Text/bytes/json/array values are bound by reference (sqlx encodes them into
/// the argument buffer immediately), so no per-value clone is paid; the
/// returned query borrows `params` for `'q`.
fn bind_pg<'q>(
    mut q: Query<'q, Postgres, PgArguments>,
    params: &'q [DbValue],
) -> Query<'q, Postgres, PgArguments> {
    for p in params {
        q = match p {
            DbValue::Null => q.bind(PgNull),
            DbValue::Bool(b) => q.bind(*b),
            DbValue::Int(i) => q.bind(*i),
            DbValue::Float(f) => q.bind(*f),
            DbValue::Text(s) => q.bind(s.as_str()),
            DbValue::Bytes(b) => q.bind(b.as_slice()),
            DbValue::Date(d) => q.bind(*d),
            DbValue::DateTime(dt) => q.bind(*dt),
            DbValue::TimestampTz(dt) => q.bind(*dt),
            DbValue::Json(j) => q.bind(j),
            DbValue::Uuid(u) => q.bind(*u),
            DbValue::Time(t) => q.bind(*t),
            // sqlx binds `Vec<T>` / `Vec<Option<T>>` as native Postgres arrays.
            DbValue::TextArray(v) => q.bind(v),
            DbValue::FloatArray(v) => q.bind(v),
            DbValue::OptFloatArray(v) => q.bind(v),
        };
    }
    q
}

/// Render one [`DbValue`] as a cell in Postgres `COPY` text format, appending to
/// `out`. NULL is the `\N` sentinel; all other values are escaped.
fn copy_render_cell(val: &DbValue, out: &mut String) {
    match val {
        DbValue::Null => out.push_str("\\N"),
        DbValue::Bool(b) => out.push(if *b { 't' } else { 'f' }),
        // Numbers contain only digits / sign / `.` / `e` — never a COPY escape
        // char — so format straight into `out`, skipping a throwaway `String`.
        DbValue::Int(i) => {
            let _ = write!(out, "{i}");
        }
        DbValue::Float(f) => {
            if f.is_nan() {
                out.push_str("NaN");
            } else if f.is_infinite() {
                out.push_str(if *f > 0.0 { "Infinity" } else { "-Infinity" });
            } else {
                let _ = write!(out, "{f}");
            }
        }
        DbValue::Text(s) => copy_escape_into(s, out),
        DbValue::Bytes(b) => {
            // bytea hex format `\x<hex>`. The backslash is COPY-escaped to `\\`,
            // and hex digits never need escaping, so write the escaped form
            // directly — no temporary `String` or per-byte allocation.
            out.push_str("\\\\x");
            for byte in b {
                out.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
                out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
            }
        }
        DbValue::Date(d) => copy_escape_into(&d.to_string(), out),
        DbValue::DateTime(dt) => copy_escape_into(&dt.to_string(), out),
        DbValue::TimestampTz(dt) => copy_escape_into(&dt.to_rfc3339(), out),
        DbValue::Json(j) => copy_escape_into(&j.to_string(), out),
        DbValue::Uuid(u) => copy_escape_into(&u.to_string(), out),
        DbValue::Time(t) => copy_escape_into(&t.to_string(), out),
        DbValue::TextArray(v) => copy_escape_into(&crate::value::pg_text_array_literal(v), out),
        DbValue::FloatArray(v) => {
            copy_escape_into(&crate::value::pg_float_array_literal(v.iter().map(|x| Some(*x))), out)
        }
        DbValue::OptFloatArray(v) => {
            copy_escape_into(&crate::value::pg_float_array_literal(v.iter().copied()), out)
        }
    }
}

/// Flush threshold for streaming COPY payloads. Rendering is flushed to the
/// sink whenever the buffer passes this size, so peak memory stays ~this bound
/// regardless of row count, and rendering overlaps with network sends.
const COPY_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Render `rows` as Postgres `COPY` text format (cells tab-separated, one row
/// per line) and stream them into an open COPY sink in ~[`COPY_CHUNK_BYTES`]
/// chunks. `ncols` is used only to pre-size the buffer.
async fn send_copy_rows<C>(
    sink: &mut sqlx::postgres::PgCopyIn<C>,
    rows: &[Vec<DbValue>],
    ncols: usize,
) -> Result<(), DbkitError>
where
    C: std::ops::DerefMut<Target = sqlx::postgres::PgConnection>,
{
    // Pre-size to the estimated payload (~12 bytes/cell + tab/newline), capped
    // at one chunk — beyond that the buffer is flushed and reused anyway.
    let estimate = rows.len() * (ncols * 12 + 1);
    let mut buf = String::with_capacity(estimate.min(COPY_CHUNK_BYTES + 1024));
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i > 0 {
                buf.push('\t');
            }
            copy_render_cell(val, &mut buf);
        }
        buf.push('\n');
        if buf.len() >= COPY_CHUNK_BYTES {
            sink.send(buf.as_bytes()).await?;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        sink.send(buf.as_bytes()).await?;
    }
    Ok(())
}

/// Escape a value for Postgres `COPY` text format (backslash, tab, newline, CR).
///
/// Scans for the next byte needing an escape and copies the clean span before
/// it in one `push_str`; a string with no escapable bytes (the common case) is
/// appended in a single copy. The four escape chars are ASCII, so byte
/// positions are always UTF-8 boundaries.
fn copy_escape_into(s: &str, out: &mut String) {
    let mut start = 0;
    for (i, b) in s.bytes().enumerate() {
        let esc = match b {
            b'\\' => "\\\\",
            b'\t' => "\\t",
            b'\n' => "\\n",
            b'\r' => "\\r",
            _ => continue,
        };
        out.push_str(&s[start..i]);
        out.push_str(esc);
        start = i + 1;
    }
    out.push_str(&s[start..]);
}

/// Core query executor for native Postgres: rich-typed transactional writes via
/// sqlx, and row-mapped analytical reads via DuckDB.
pub struct PgHandler {
    pool: PgPool,
    #[cfg(feature = "duckdb")]
    duck: Option<DuckEngine>,
}

impl PgHandler {
    /// Create a handler for writes against the given native Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            #[cfg(feature = "duckdb")]
            duck: None,
        }
    }

    /// Create a handler with an in-memory DuckDB analytical read engine.
    #[cfg(feature = "duckdb")]
    pub fn with_duckdb(pool: PgPool) -> Result<Self, DbkitError> {
        Ok(Self {
            pool,
            duck: Some(DuckEngine::new_in_memory()?),
        })
    }

    /// Create a handler with DuckDB and a live Postgres attachment, so DuckDB
    /// queries the Postgres tables directly via the `pg` catalog
    /// (`SELECT … FROM pg.<schema>.<table>`) without an explicit sync.
    #[cfg(feature = "duckdb")]
    pub fn with_duckdb_attached_postgres(
        pool: PgPool,
        pg_connection_string: &str,
    ) -> Result<Self, DbkitError> {
        let duck = DuckEngine::new_in_memory()?;
        duck.attach_postgres(pg_connection_string)?;
        Ok(Self {
            pool,
            duck: Some(duck),
        })
    }

    /// Whether a DuckDB read engine is attached.
    pub fn has_read_engine(&self) -> bool {
        #[cfg(feature = "duckdb")]
        {
            self.duck.is_some()
        }
        #[cfg(not(feature = "duckdb"))]
        {
            false
        }
    }

    /// Get a reference to the native Postgres write pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Accent-insensitive name key: NFD-decompose, DROP combining marks, then
    /// lowercase — "José Ramírez" and "Jose Ramirez" produce the same key.
    /// See `BaseHandler::normalize_name` for the rationale (NFD alone leaves
    /// combining marks, so accented names never matched stripped variants).
    pub fn normalize_name(name: &str) -> String {
        use unicode_normalization::char::is_combining_mark;
        name.nfd()
            .filter(|c| !is_combining_mark(*c))
            .collect::<String>()
            .to_lowercase()
    }

    // ==================== UNIFIED WRITE ====================

    /// Execute a write operation against the Postgres pool. Placeholders are
    /// Postgres-native (`$1, $2, …`).
    pub async fn execute_write(
        &self,
        op: WriteOp<'_>,
    ) -> Result<QueryResult<PgRow>, DbkitError> {
        match op {
            WriteOp::Single {
                query,
                params,
                mode,
            } => self.query(query, params, mode).await,

            WriteOp::BatchDDL { queries } => {
                let mut tx = self.pool.begin().await?;
                for query in queries {
                    sqlx::query(AssertSqlSafe(*query)).execute(&mut *tx).await?;
                }
                tx.commit().await?;
                Ok(QueryResult::None)
            }

            WriteOp::BatchParams {
                query,
                params_list,
                isolate_rows,
            } => {
                if params_list.is_empty() {
                    return Ok(QueryResult::None);
                }
                let total = params_list.len();
                let mut tx = self.pool.begin().await?;

                if !isolate_rows {
                    // Fast path: no per-row SAVEPOINT. The whole batch is one
                    // transaction, so any error rolls back *everything*
                    // (all-or-nothing) — the cost of dropping savepoints, but
                    // ~2× faster than the isolated path below.
                    //
                    // Statement reuse: a typeless NULL (`PgNull`, OID 0) lets the
                    // server pin the cached statement's parameter type from the
                    // first *cached* row, so a later row binding a concrete type
                    // for that same column fails with 22P03. Guard per ROW (not
                    // per batch): rows with a NULL re-parse individually
                    // (`persistent(false)`, letting the server infer that row's
                    // NULL from context), while null-free rows — whose concrete
                    // types are mutually consistent — keep reusing one cached
                    // prepared statement. A batch that is 10% NULL rows keeps
                    // statement reuse for the other 90%.
                    for params in &params_list {
                        let has_null = params.iter().any(|v| matches!(v, DbValue::Null));
                        bind_pg(sqlx::query(AssertSqlSafe(query)), params)
                            .persistent(!has_null)
                            .execute(&mut *tx)
                            .await?;
                    }
                    tx.commit().await?;
                    return Ok(QueryResult::None);
                }

                let mut failed = 0usize;
                for (idx, params) in params_list.iter().enumerate() {
                    // Wrap each row in a SAVEPOINT so a bad row rolls back on its
                    // own instead of aborting the whole transaction. Without this,
                    // Postgres marks the transaction failed on the first error and
                    // every following row dies with 25P02 ("current transaction is
                    // aborted"), turning one bad row into a whole failed batch.
                    sqlx::query(AssertSqlSafe("SAVEPOINT dbkit_row"))
                        .execute(&mut *tx)
                        .await?;
                    // `.persistent(false)` re-parses per row instead of reusing one
                    // cached prepared statement across the batch. Reuse pins each
                    // parameter's type from the FIRST row: a row whose value is a
                    // typeless NULL lets the server resolve that param to the column
                    // type (e.g. int4), and a later row binding the same column's
                    // value as int8 then fails with 22P03 ("incorrect binary data
                    // format"). Per-row parse keeps each row's param types self-consistent.
                    let q = bind_pg(sqlx::query(AssertSqlSafe(query)), params).persistent(false);
                    match q.execute(&mut *tx).await {
                        Ok(_) => {
                            sqlx::query(AssertSqlSafe("RELEASE SAVEPOINT dbkit_row"))
                                .execute(&mut *tx)
                                .await?;
                        }
                        Err(e) => {
                            warn!("BatchParams row {}/{} failed: {:?}", idx + 1, total, e);
                            failed += 1;
                            sqlx::query(AssertSqlSafe("ROLLBACK TO SAVEPOINT dbkit_row"))
                                .execute(&mut *tx)
                                .await?;
                            sqlx::query(AssertSqlSafe("RELEASE SAVEPOINT dbkit_row"))
                                .execute(&mut *tx)
                                .await?;
                        }
                    }
                }
                tx.commit().await?;
                if failed > 0 {
                    warn!(
                        "BatchParams: {}/{} succeeded, {} failed",
                        total - failed,
                        total,
                        failed
                    );
                }
                Ok(QueryResult::None)
            }
        }
    }

    /// Bulk-insert rows via Postgres `COPY ... FROM STDIN` (text format).
    ///
    /// **The fastest way to load many rows** — one streamed `COPY` instead of a
    /// parse + execute (+ savepoint) per row like [`WriteOp::BatchParams`].
    /// Benchmarks at roughly 30–50× the throughput of `BatchParams`. Each row in
    /// `rows` must align positionally with `columns`. Returns the number of rows
    /// copied.
    ///
    /// # `copy_in` vs [`WriteOp::BatchParams`] — which to use
    ///
    /// | Reach for `copy_in` when… | Reach for `BatchParams` when… |
    /// |---|---|
    /// | Plain bulk insert into one table | You need `INSERT … ON CONFLICT` (upsert) |
    /// | Data is trusted; all-or-nothing is fine | You need per-row isolation (skip bad rows) |
    /// | You want maximum throughput | The statement isn't a plain insert (`UPDATE`, `RETURNING`, computed `VALUES`) |
    /// | Target is Postgres | Target is a non-Postgres backend (use the `Any` pool) |
    ///
    /// `COPY` is **not** an `INSERT` statement, so it does **not** support
    /// `ON CONFLICT`, `RETURNING`, `DEFAULT` expressions, or `WHERE`, and it is
    /// **all-or-nothing**: a constraint violation aborts the entire load (it does
    /// not skip bad rows like `BatchParams`).
    ///
    /// To bulk-**upsert**, combine the two: `COPY` into a constraint-free staging
    /// table, then run one set-based `INSERT … SELECT … ON CONFLICT` — far faster
    /// than per-row `BatchParams` with `ON CONFLICT`:
    ///
    /// ```sql
    /// CREATE TEMP TABLE stage (LIKE target INCLUDING DEFAULTS) ON COMMIT DROP;
    /// COPY stage (id, name) FROM STDIN;            -- fast bulk load, no constraints
    /// INSERT INTO target (id, name)
    ///   SELECT id, name FROM stage                 -- one set-based upsert
    ///   ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
    /// ```
    pub async fn copy_in(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<DbValue>],
    ) -> Result<u64, DbkitError> {
        use sqlx::postgres::PgPoolCopyExt;

        if rows.is_empty() {
            return Ok(0);
        }

        let stmt = format!("COPY {table} ({}) FROM STDIN", columns.join(", "));

        let mut sink = self.pool.copy_in_raw(&stmt).await?;
        send_copy_rows(&mut sink, rows, columns.len()).await?;
        Ok(sink.finish().await?)
    }

    /// Bulk-**upsert** rows: `COPY` into a staging table, then one set-based
    /// `INSERT … SELECT … ON CONFLICT`, all in a single transaction.
    ///
    /// This is the fast path for `ON CONFLICT` at scale. Plain [`copy_in`] can't
    /// do `ON CONFLICT` (it's not an `INSERT`), and per-row
    /// [`WriteOp::BatchParams`] with `ON CONFLICT` pays per-row overhead. This
    /// combines both strengths: COPY's bulk ingestion into a constraint-free
    /// staging table, then a single set-based upsert into the target.
    ///
    /// - `columns` — columns present in `rows` (positional), copied into staging.
    /// - `conflict_columns` — the conflict target (must back a unique/PK index).
    /// - `update_columns` — columns to overwrite on conflict (set to the incoming
    ///   `EXCLUDED` value). **Empty** ⇒ `DO NOTHING` (insert-or-ignore).
    ///
    /// Returns the number of rows inserted or updated.
    ///
    /// The staging table is `CREATE TEMP TABLE … AS SELECT {columns} FROM target
    /// WITH NO DATA` (`ON COMMIT DROP`) — target column types, no constraints or
    /// defaults — and vanishes at commit. The
    /// final upsert is all-or-nothing: a non-conflict error (CHECK/FK/type) aborts
    /// the batch. **Within a single call, `conflict_columns` must be unique across
    /// `rows`** — duplicate keys make `ON CONFLICT DO UPDATE` error with "command
    /// cannot affect row a second time"; de-duplicate before calling.
    ///
    /// [`copy_in`]: Self::copy_in
    pub async fn copy_upsert(
        &self,
        table: &str,
        columns: &[&str],
        conflict_columns: &[&str],
        update_columns: &[&str],
        rows: &[Vec<DbValue>],
    ) -> Result<u64, DbkitError> {
        if rows.is_empty() {
            return Ok(0);
        }
        if conflict_columns.is_empty() {
            // Would render `ON CONFLICT () …` — a Postgres syntax error, but an
            // opaque one; fail with a message that names the actual mistake.
            return Err(DbkitError::InvalidArgument(
                "copy_upsert: conflict_columns must not be empty".into(),
            ));
        }

        let cols = columns.join(", ");
        let stage = "dbkit_copy_stage";

        let on_conflict = if update_columns.is_empty() {
            format!("ON CONFLICT ({}) DO NOTHING", conflict_columns.join(", "))
        } else {
            let set = update_columns
                .iter()
                .map(|c| format!("{c} = EXCLUDED.{c}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ON CONFLICT ({}) DO UPDATE SET {set}", conflict_columns.join(", "))
        };

        let mut tx = self.pool.begin().await?;

        // 1. Staging table with ONLY the copied columns (target types, no
        //    constraints, no defaults), dropped at COMMIT. Temp tables are
        //    connection-scoped, so the fixed name is safe even under concurrent
        //    callers on separate connections.
        //
        //    `AS SELECT … WITH NO DATA` rather than `LIKE {table}`: LIKE always
        //    copies NOT NULL constraints (so unlisted NOT-NULL columns would
        //    reject COPY's NULL fill), and `INCLUDING DEFAULTS` — used before
        //    0.5 to paper over that — made COPY fire a copied serial default,
        //    burning one target-sequence value per staged row for nothing.
        //    Narrowing staging to the listed columns sidesteps both; the
        //    target's own defaults still apply at the INSERT below.
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TEMP TABLE {stage} ON COMMIT DROP AS SELECT {cols} FROM {table} WITH NO DATA"
        )))
        .execute(&mut *tx)
        .await?;

        // 2. Bulk-load into staging via COPY on the SAME connection (so the temp
        //    table is visible) — this is where the throughput comes from.
        let mut copy = (*tx)
            .copy_in_raw(&format!("COPY {stage} ({cols}) FROM STDIN"))
            .await?;
        send_copy_rows(&mut copy, rows, columns.len()).await?;
        copy.finish().await?;

        // 3. One set-based upsert from staging into the target.
        let result = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {table} ({cols}) SELECT {cols} FROM {stage} {on_conflict}"
        )))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(result.rows_affected())
    }

    // ==================== NATIVE POSTGRES READ ====================

    /// Run a query against the native Postgres pool, returning rows per `mode`.
    ///
    /// This is the OLTP read path — single-row lookups and small result sets go
    /// straight to Postgres (one round-trip → [`PgRow`]), no analytical engine.
    /// Placeholders are Postgres-native (`$1, $2, …`); read columns off the
    /// returned [`PgRow`]s with `row.get(i)` / `row.try_get(i)`.
    pub async fn query(
        &self,
        query: &str,
        params: Vec<DbValue>,
        mode: FetchMode,
    ) -> Result<QueryResult<PgRow>, DbkitError> {
        // Statement reuse hazard: with the default `persistent(true)`, sqlx
        // caches one prepared statement per (connection, SQL). A typeless NULL
        // (`PgNull`, OID 0) lets the server pin that parameter's type from the
        // FIRST execution, so a later call binding a concrete type for the same
        // column fails with 22P03 ("incorrect binary data format"). Reuse the
        // cached statement only when this call has no NULLs; otherwise re-parse
        // so each call's param types stay self-consistent. (Same guard as the
        // `BatchParams` write path.)
        let has_null = params.iter().any(|v| matches!(v, DbValue::Null));
        let q = bind_pg(sqlx::query(AssertSqlSafe(query)), &params).persistent(!has_null);
        match mode {
            FetchMode::None => {
                q.execute(&self.pool).await?;
                Ok(QueryResult::None)
            }
            FetchMode::One => Ok(QueryResult::One(q.fetch_one(&self.pool).await?)),
            FetchMode::Optional => Ok(QueryResult::Optional(q.fetch_optional(&self.pool).await?)),
            FetchMode::All => Ok(QueryResult::All(q.fetch_all(&self.pool).await?)),
        }
    }

    // ==================== ANALYTICAL READ (DuckDB / Arrow) ====================

    /// Run an analytical query against the attached DuckDB engine, returning
    /// columnar Arrow [`RecordBatch`]es. For large joins/aggregations consumed as
    /// DataFrames. Errors with [`DbkitError::NoReadEngine`] if no engine is
    /// attached. For typed rows, deserialize the batches (see
    /// [`BaseHandler::execute_read_as`](crate::BaseHandler::execute_read_as)).
    #[cfg(feature = "duckdb")]
    pub async fn execute_read(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<Vec<RecordBatch>, DbkitError> {
        self.duck
            .as_ref()
            .ok_or(DbkitError::NoReadEngine)?
            .query_arrow(sql, params)
            .await
    }

    /// Like [`execute_read`](Self::execute_read) but deserializes each row into
    /// `T` via `serde_arrow` — the typed analytical read. `T`'s field names must
    /// match the query's output column names. Use for DuckDB-side analytical
    /// reads (large scans / aggregations) that map to typed rows. Errors with
    /// [`DbkitError::NoReadEngine`] if no engine is attached.
    #[cfg(feature = "duckdb")]
    pub async fn execute_read_as<T>(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<Vec<T>, DbkitError>
    where
        T: serde::de::DeserializeOwned,
    {
        let batches = self.execute_read(sql, params).await?;
        crate::analytical::deserialize_batches(&batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn esc(s: &str) -> String {
        let mut out = String::new();
        copy_escape_into(s, &mut out);
        out
    }

    #[test]
    fn copy_escape_clean_passthrough_and_escapes() {
        assert_eq!(esc(""), "");
        assert_eq!(esc("plain text é ✓ 日本"), "plain text é ✓ 日本");
        assert_eq!(esc("a\tb"), "a\\tb");
        assert_eq!(esc("\\"), "\\\\");
        assert_eq!(esc("\n\r\t"), "\\n\\r\\t");
        assert_eq!(esc("trailing\\"), "trailing\\\\");
        assert_eq!(esc("\\leading"), "\\\\leading");
        assert_eq!(esc("mixé\tmulti✓\nbyte"), "mixé\\tmulti✓\\nbyte");
    }
}
