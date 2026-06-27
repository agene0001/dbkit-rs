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
            } => {
                if params_list.is_empty() {
                    return Ok(QueryResult::None);
                }
                let total = params_list.len();
                let mut tx = self.pool.begin().await?;
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
