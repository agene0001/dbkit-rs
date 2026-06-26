use crate::DbkitError;
use crate::value::DbValue;
use sqlx::any::{AnyArguments, AnyRow};
use sqlx::query::Query;
use sqlx::{Any, AnyPool, AssertSqlSafe};
use tracing::warn;
use unicode_normalization::UnicodeNormalization;

#[cfg(any(feature = "duckdb", feature = "datafusion"))]
use crate::analytical::RecordBatch;
#[cfg(any(feature = "duckdb", feature = "datafusion"))]
use crate::read::ReadEngine;

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

/// Unified write operation types.
pub enum WriteOp<'a> {
    /// Single query with optional return.
    Single {
        query: &'a str,
        params: Vec<DbValue>,
        mode: FetchMode,
    },
    /// Batch of DDL statements executed in a single transaction.
    BatchDDL { queries: &'a [&'a str] },
    /// Same query executed for each parameter set in a transaction.
    BatchParams {
        query: &'a str,
        params_list: Vec<Vec<DbValue>>,
    },
}

// ---------------------------------------------------------------------------
// Query result types
// ---------------------------------------------------------------------------

/// How many rows to expect from a query.
#[derive(Debug, Clone, Copy)]
pub enum FetchMode {
    None,
    One,
    Optional,
    All,
}

/// Result wrapper for write queries.
pub enum QueryResult<T> {
    None,
    One(T),
    Optional(Option<T>),
    All(Vec<T>),
}

impl<T> QueryResult<T> {
    pub fn one(self) -> Result<T, DbkitError> {
        match self {
            Self::One(v) => Ok(v),
            _ => Err(DbkitError::RowCount {
                expected: "One".into(),
                actual: 0,
            }),
        }
    }

    pub fn optional(self) -> Result<Option<T>, DbkitError> {
        match self {
            Self::Optional(v) => Ok(v),
            Self::One(v) => Ok(Some(v)),
            Self::None => Ok(None),
            _ => Err(DbkitError::RowCount {
                expected: "Optional".into(),
                actual: 0,
            }),
        }
    }

    pub fn all(self) -> Result<Vec<T>, DbkitError> {
        match self {
            Self::All(v) => Ok(v),
            _ => Err(DbkitError::RowCount {
                expected: "All".into(),
                actual: 0,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Parameter binding
// ---------------------------------------------------------------------------

/// Bind a slice of [`DbValue`]s onto a sqlx query, in order.
///
/// Values are bound by owned copy, so the returned query does not borrow
/// `params`.
fn bind_params<'q>(
    mut q: Query<'q, Any, AnyArguments>,
    params: &[DbValue],
) -> Query<'q, Any, AnyArguments> {
    for p in params {
        q = match p {
            DbValue::Null => q.bind(Option::<i64>::None),
            DbValue::Bool(b) => q.bind(*b),
            DbValue::Int(i) => q.bind(*i),
            DbValue::Float(f) => q.bind(*f),
            DbValue::Text(s) => q.bind(s.clone()),
            DbValue::Bytes(b) => q.bind(b.clone()),
        };
    }
    q
}

// ---------------------------------------------------------------------------
// BaseHandler
// ---------------------------------------------------------------------------

/// Core query executor: transactional writes via sqlx, and optionally
/// analytical reads via a pluggable [`ReadEngine`] (DuckDB or DataFusion).
pub struct BaseHandler {
    pool: AnyPool,
    #[cfg(any(feature = "duckdb", feature = "datafusion"))]
    read_engine: Option<Box<dyn ReadEngine>>,
}

impl BaseHandler {
    /// Create a handler for writes against the given sqlx pool.
    pub fn new(pool: AnyPool) -> Self {
        Self {
            pool,
            #[cfg(any(feature = "duckdb", feature = "datafusion"))]
            read_engine: None,
        }
    }

    /// Create a handler with an in-memory DuckDB analytical read engine.
    #[cfg(feature = "duckdb")]
    pub fn with_duckdb(pool: AnyPool) -> Result<Self, DbkitError> {
        let engine = crate::read::duckdb::DuckEngine::new_in_memory()?;
        Ok(Self {
            pool,
            read_engine: Some(Box::new(engine)),
        })
    }

    /// Create a handler with DuckDB and a live Postgres attachment.
    ///
    /// DuckDB queries the Postgres tables directly via the `pg` catalog
    /// (`SELECT … FROM pg.<schema>.<table>`) without an explicit sync — the
    /// pre-rewrite zero-copy `ATTACH` pipeline. You can still also `sync_*`
    /// tables into local memory for faster repeated analytics.
    #[cfg(feature = "duckdb")]
    pub fn with_duckdb_attached_postgres(
        pool: AnyPool,
        pg_connection_string: &str,
    ) -> Result<Self, DbkitError> {
        let engine = crate::read::duckdb::DuckEngine::new_in_memory()?;
        engine.attach_postgres(pg_connection_string)?;
        Ok(Self {
            pool,
            read_engine: Some(Box::new(engine)),
        })
    }

    /// Create a handler with a DataFusion analytical read engine.
    #[cfg(feature = "datafusion")]
    pub fn with_datafusion(pool: AnyPool) -> Result<Self, DbkitError> {
        let engine = crate::read::datafusion::DataFusionEngine::new();
        Ok(Self {
            pool,
            read_engine: Some(Box::new(engine)),
        })
    }

    /// Whether an analytical read engine is attached.
    pub fn has_read_engine(&self) -> bool {
        #[cfg(any(feature = "duckdb", feature = "datafusion"))]
        {
            self.read_engine.is_some()
        }
        #[cfg(not(any(feature = "duckdb", feature = "datafusion")))]
        {
            false
        }
    }

    /// Get a reference to the write pool.
    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    /// Unicode NFD normalization — decomposes characters then lowercases.
    /// Useful for matching names with different Unicode representations.
    pub fn normalize_name(name: &str) -> String {
        name.nfd().collect::<String>().to_lowercase()
    }

    // ==================== UNIFIED WRITE ====================

    /// Execute a write operation against the transactional pool.
    ///
    /// Placeholders are backend-native: `$1, $2, …` for Postgres, `?` for
    /// MySQL/SQLite. sqlx's `Any` driver does not rewrite them, so write the
    /// SQL for the backend you connected to.
    pub async fn execute_write(
        &self,
        op: WriteOp<'_>,
    ) -> Result<QueryResult<AnyRow>, DbkitError> {
        match op {
            WriteOp::Single {
                query,
                params,
                mode,
            } => {
                let q = bind_params(sqlx::query(AssertSqlSafe(query)), &params);
                match mode {
                    FetchMode::None => {
                        q.execute(&self.pool).await?;
                        Ok(QueryResult::None)
                    }
                    FetchMode::One => {
                        let row = q.fetch_one(&self.pool).await?;
                        Ok(QueryResult::One(row))
                    }
                    FetchMode::Optional => {
                        let row = q.fetch_optional(&self.pool).await?;
                        Ok(QueryResult::Optional(row))
                    }
                    FetchMode::All => {
                        let rows = q.fetch_all(&self.pool).await?;
                        Ok(QueryResult::All(rows))
                    }
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
                    // sqlx caches the prepared statement per query string on the
                    // connection, so re-issuing the same query reuses it.
                    let q = bind_params(sqlx::query(AssertSqlSafe(query)), params);
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

    // ==================== UNIFIED READ ====================

    /// Execute an analytical query against the attached read engine, returning
    /// columnar Arrow [`RecordBatch`]es.
    ///
    /// Returns [`DbkitError::NoReadEngine`] if no engine is attached.
    #[cfg(any(feature = "duckdb", feature = "datafusion"))]
    pub async fn execute_read(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<Vec<RecordBatch>, DbkitError> {
        self.read_engine
            .as_ref()
            .ok_or(DbkitError::NoReadEngine)?
            .query_arrow(sql, params)
            .await
    }

    /// Execute an analytical query and deserialize each row into `T`.
    ///
    /// This is the typed-read replacement for the old closure-mapped
    /// `ReadOp::Standard`: instead of a `|row| …` closure, derive
    /// `serde::Deserialize` on your row struct. Works for any read engine,
    /// since it deserializes from the Arrow batches via `serde_arrow`.
    ///
    /// ```ignore
    /// #[derive(serde::Deserialize)]
    /// struct Item { name: String, qty: i64 }
    /// let items: Vec<Item> = handler.execute_read_as("SELECT name, qty FROM items", &[]).await?;
    /// ```
    #[cfg(any(feature = "duckdb", feature = "datafusion"))]
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

    // ==================== SYNC (transactional -> analytical) ====================

    /// Run a query against the transactional pool and load its result into the
    /// analytical engine as a named in-memory table.
    ///
    /// This is the engine-agnostic replacement for the old DuckDB `ATTACH`
    /// sync: rows are fetched over sqlx, converted to Arrow, and handed to the
    /// active read engine. Works for any backend × engine combination.
    #[cfg(any(feature = "duckdb", feature = "datafusion"))]
    pub async fn sync_query(
        &self,
        name: &str,
        query: &str,
        params: &[DbValue],
    ) -> Result<(), DbkitError> {
        let engine = self.read_engine.as_ref().ok_or(DbkitError::NoReadEngine)?;

        let q = bind_params(sqlx::query(AssertSqlSafe(query.to_string())), params);
        let rows = q.fetch_all(&self.pool).await?;

        if let Some(batch) = crate::read::rows_to_record_batch(&rows)? {
            engine.load_table(name, vec![batch]).await?;
        }
        Ok(())
    }

    /// Copy entire tables from the transactional store into the analytical
    /// engine, one table per name (`SELECT * FROM {table}`).
    #[cfg(any(feature = "duckdb", feature = "datafusion"))]
    pub async fn sync_tables(&self, tables: &[&str]) -> Result<(), DbkitError> {
        for table in tables {
            self.sync_query(table, &format!("SELECT * FROM {table}"), &[])
                .await?;
        }
        Ok(())
    }
}
