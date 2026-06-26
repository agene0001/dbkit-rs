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
use crate::read::{ReadOp, ReadResult, duckdb::DuckEngine};

/// Bind a slice of [`DbValue`]s onto a sqlx Postgres query, in order, binding
/// the rich variants to their native Postgres types (no text fallback). Values
/// are bound by owned copy, so the returned query does not borrow `params`.
fn bind_pg<'q>(
    mut q: Query<'q, Postgres, PgArguments>,
    params: &[DbValue],
) -> Query<'q, Postgres, PgArguments> {
    for p in params {
        q = match p {
            DbValue::Null => q.bind(Option::<i64>::None),
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
            } => {
                let q = bind_pg(sqlx::query(AssertSqlSafe(query)), &params);
                match mode {
                    FetchMode::None => {
                        q.execute(&self.pool).await?;
                        Ok(QueryResult::None)
                    }
                    FetchMode::One => Ok(QueryResult::One(q.fetch_one(&self.pool).await?)),
                    FetchMode::Optional => {
                        Ok(QueryResult::Optional(q.fetch_optional(&self.pool).await?))
                    }
                    FetchMode::All => Ok(QueryResult::All(q.fetch_all(&self.pool).await?)),
                }
            }

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
                    let q = bind_pg(sqlx::query(AssertSqlSafe(query)), params);
                    if let Err(e) = q.execute(&mut *tx).await {
                        warn!("BatchParams row {}/{} failed: {:?}", idx + 1, total, e);
                        failed += 1;
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

    // ==================== UNIFIED READ (DuckDB) ====================

    /// Execute a read operation against the attached DuckDB engine — either a
    /// row-mapped [`ReadOp::Standard`] (typed via `map_fn`) or an
    /// [`ReadOp::Arrow`] columnar read. Errors with
    /// [`DbkitError::NoReadEngine`] if no engine is attached.
    #[cfg(feature = "duckdb")]
    pub async fn execute_read<T, F>(
        &self,
        op: ReadOp<'_, T, F>,
    ) -> Result<ReadResult<T>, DbkitError>
    where
        F: Fn(&::duckdb::Row<'_>) -> Result<T, DbkitError> + Send + 'static,
        T: Send + 'static,
    {
        use crate::read::ReadEngine;
        let duck = self.duck.as_ref().ok_or(DbkitError::NoReadEngine)?;
        match op {
            ReadOp::Standard {
                query,
                params,
                map_fn,
                mode,
            } => Ok(ReadResult::Standard(
                duck.query_mapped(query, &params, map_fn, mode).await?,
            )),
            ReadOp::Arrow { query, params } => {
                Ok(ReadResult::Arrow(duck.query_arrow(query, &params).await?))
            }
        }
    }
}
