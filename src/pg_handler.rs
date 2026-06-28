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
/// the rich variants to their native Postgres types (no text fallback). Values
/// are bound by owned copy, so the returned query does not borrow `params`.
fn bind_pg<'q>(
    mut q: Query<'q, Postgres, PgArguments>,
    params: &[DbValue],
) -> Query<'q, Postgres, PgArguments> {
    for p in params {
        q = match p {
            DbValue::Null => q.bind(PgNull),
            DbValue::Bool(b) => q.bind(*b),
            DbValue::Int(i) => q.bind(*i),
            DbValue::Float(f) => q.bind(*f),
            DbValue::Text(s) => q.bind(s.clone()),
            DbValue::Bytes(b) => q.bind(b.clone()),
            DbValue::Date(d) => q.bind(*d),
            DbValue::DateTime(dt) => q.bind(*dt),
            DbValue::TimestampTz(dt) => q.bind(*dt),
            DbValue::Json(j) => q.bind(j.clone()),
            DbValue::Uuid(u) => q.bind(*u),
            DbValue::Time(t) => q.bind(*t),
            // sqlx binds `Vec<T>` / `Vec<Option<T>>` as native Postgres arrays.
            DbValue::TextArray(v) => q.bind(v.clone()),
            DbValue::FloatArray(v) => q.bind(v.clone()),
            DbValue::OptFloatArray(v) => q.bind(v.clone()),
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

/// Render `rows` as a Postgres `COPY` text-format payload: cells tab-separated,
/// one row per line. `ncols` is used only to pre-size the buffer.
fn render_copy_text(rows: &[Vec<DbValue>], ncols: usize) -> String {
    // Pre-size the buffer to avoid repeated grow-and-copy reallocations as it
    // fills (~12 bytes/cell + tab/newline is a rough but useful estimate).
    let mut payload = String::with_capacity(rows.len() * (ncols * 12 + 1));
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i > 0 {
                payload.push('\t');
            }
            copy_render_cell(val, &mut payload);
        }
        payload.push('\n');
    }
    payload
}

/// Escape a value for Postgres `COPY` text format (backslash, tab, newline, CR).
fn copy_escape_into(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
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

    /// Unicode NFD normalization — decomposes characters then lowercases.
    pub fn normalize_name(name: &str) -> String {
        name.nfd().collect::<String>().to_lowercase()
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
                    // FIRST row, so a later row binding a concrete type for that
                    // same column fails with 22P03. So reuse one prepared
                    // statement (`persistent(true)`) only when the batch has no
                    // NULLs; otherwise re-parse per row to keep each row's param
                    // types self-consistent (matching the isolated path).
                    let has_null = params_list
                        .iter()
                        .any(|row| row.iter().any(|v| matches!(v, DbValue::Null)));
                    let persistent = !has_null;
                    for params in &params_list {
                        bind_pg(sqlx::query(AssertSqlSafe(query)), params)
                            .persistent(persistent)
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
        let payload = render_copy_text(rows, columns.len());

        let mut sink = self.pool.copy_in_raw(&stmt).await?;
        sink.send(payload.as_bytes()).await?;
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
    /// The staging table is `CREATE TEMP TABLE … (LIKE target INCLUDING DEFAULTS)
    /// ON COMMIT DROP`, so it copies no constraints and vanishes at commit. The
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

        // 1. Staging table shaped like the target (no constraints), dropped at
        //    COMMIT. Temp tables are connection-scoped, so the fixed name is safe
        //    even under concurrent callers on separate connections.
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TEMP TABLE {stage} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )))
        .execute(&mut *tx)
        .await?;

        // 2. Bulk-load into staging via COPY on the SAME connection (so the temp
        //    table is visible) — this is where the throughput comes from.
        let payload = render_copy_text(rows, columns.len());
        let mut copy = (&mut *tx)
            .copy_in_raw(&format!("COPY {stage} ({cols}) FROM STDIN"))
            .await?;
        copy.send(payload.as_bytes()).await?;
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
        let q = bind_pg(sqlx::query(AssertSqlSafe(query)), &params);
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
