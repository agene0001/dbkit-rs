use crate::DbkitError;
use deadpool_postgres::Pool;
#[cfg(feature = "duckdb")]
use duckdb::Connection as DuckConnection;
use std::sync::Arc;
#[cfg(feature = "duckdb")]
use std::sync::Mutex;
#[cfg(feature = "duckdb")]
use tokio::task;
use tokio_postgres::Row as PgRow;
use tokio_postgres::types::ToSql;
use tracing::warn;
use unicode_normalization::UnicodeNormalization;

#[cfg(feature = "duckdb")]
pub use duckdb::arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Write operations (Postgres)
// ---------------------------------------------------------------------------

/// Unified write operation types for Postgres.
pub enum WriteOp<'a> {
    /// Single query with optional return.
    Single {
        query: &'a str,
        params: &'a [&'a (dyn ToSql + Sync)],
        mode: FetchMode,
    },
    /// Batch of DDL statements executed in a single transaction.
    BatchDDL { queries: &'a [&'a str] },
    /// Same query executed for each parameter set in a transaction.
    BatchParams {
        query: &'a str,
        params_list: Vec<Vec<Box<dyn ToSql + Sync + Send>>>,
    },
}

// ---------------------------------------------------------------------------
// Read operations (DuckDB)
// ---------------------------------------------------------------------------

/// Unified read operation types for DuckDB.
#[cfg(feature = "duckdb")]
pub enum ReadOp<'a, T, F>
where
    F: Fn(&duckdb::Row<'_>) -> Result<T, DbkitError> + Send + 'static,
    T: Send + 'static,
{
    /// Standard mapped query.
    Standard {
        query: &'a str,
        params: Vec<DuckParam>,
        map_fn: F,
        mode: FetchMode,
    },
}

/// DuckDB parameter types (including optional variants).
#[cfg(feature = "duckdb")]
#[derive(Debug, Clone)]
pub enum DuckParam {
    Int(i32),
    Int64(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
    OptInt(Option<i32>),
    OptInt64(Option<i64>),
    OptFloat(Option<f64>),
    OptText(Option<String>),
    OptBool(Option<bool>),
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

/// Result wrapper for Postgres write queries.
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

/// Result wrapper for DuckDB read queries.
#[cfg(feature = "duckdb")]
pub enum ReadResult<T> {
    Standard(QueryResult<T>),
}

#[cfg(feature = "duckdb")]
impl<T> ReadResult<T> {
    pub fn standard(self) -> Result<QueryResult<T>, DbkitError> {
        match self {
            Self::Standard(qr) => Ok(qr),
        }
    }
}

// ---------------------------------------------------------------------------
// BaseHandler
// ---------------------------------------------------------------------------

/// Core query executor for Postgres writes and optionally DuckDB reads.
pub struct BaseHandler {
    pg_pool: Arc<Pool>,
    #[cfg(feature = "duckdb")]
    duck_conn: Option<Arc<Mutex<DuckConnection>>>,
}

impl BaseHandler {
    /// Create a handler with Postgres only (for writes).
    pub fn new(pg_pool: Arc<Pool>) -> Self {
        Self {
            pg_pool,
            #[cfg(feature = "duckdb")]
            duck_conn: None,
        }
    }

    /// Create a handler with Postgres + DuckDB attached (for reads + writes).
    #[cfg(feature = "duckdb")]
    pub fn with_duckdb(
        pg_pool: Arc<Pool>,
        pg_connection_string: &str,
    ) -> Result<Self, DbkitError> {
        let duck_conn = DuckConnection::open_in_memory()
            .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

        duck_conn
            .execute_batch("INSTALL postgres; LOAD postgres;")
            .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

        duck_conn
            .execute(
                &format!(
                    "ATTACH '{}' AS pg (TYPE POSTGRES)",
                    pg_connection_string
                ),
                [],
            )
            .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

        duck_conn
            .execute("USE pg", [])
            .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

        Ok(Self {
            pg_pool,
            duck_conn: Some(Arc::new(Mutex::new(duck_conn))),
        })
    }

    /// Whether DuckDB is attached for reads.
    pub fn has_duckdb(&self) -> bool {
        #[cfg(feature = "duckdb")]
        {
            self.duck_conn.is_some()
        }
        #[cfg(not(feature = "duckdb"))]
        {
            false
        }
    }

    /// Get a reference to the Postgres pool.
    pub fn pool(&self) -> &Arc<Pool> {
        &self.pg_pool
    }

    /// Unicode NFD normalization — decomposes characters then lowercases.
    /// Useful for matching names with different Unicode representations.
    pub fn normalize_name(name: &str) -> String {
        name.nfd().collect::<String>().to_lowercase()
    }

    // ==================== UNIFIED WRITE ====================

    /// Execute a write operation against Postgres.
    pub async fn execute_write(
        &self,
        op: WriteOp<'_>,
    ) -> Result<QueryResult<PgRow>, DbkitError> {
        let mut client = self
            .pg_pool
            .get()
            .await
            .map_err(|e| DbkitError::Pool(e.to_string()))?;

        match op {
            WriteOp::Single {
                query,
                params,
                mode,
            } => match mode {
                FetchMode::None => {
                    client.execute(query, params).await?;
                    Ok(QueryResult::None)
                }
                FetchMode::One => {
                    let row = client.query_one(query, params).await?;
                    Ok(QueryResult::One(row))
                }
                FetchMode::Optional => {
                    let row = client.query_opt(query, params).await?;
                    Ok(QueryResult::Optional(row))
                }
                FetchMode::All => {
                    let rows = client.query(query, params).await?;
                    Ok(QueryResult::All(rows))
                }
            },

            WriteOp::BatchDDL { queries } => {
                let transaction = client.transaction().await?;

                for query in queries {
                    transaction.execute(*query, &[]).await?;
                }

                transaction.commit().await?;
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
                let transaction = client.transaction().await?;
                let stmt = transaction.prepare(query).await?;
                let mut failed = 0usize;

                let max_params = params_list.first().map(|p| p.len()).unwrap_or(0);
                let mut params_refs: Vec<&(dyn ToSql + Sync)> =
                    Vec::with_capacity(max_params);

                for (idx, params) in params_list.iter().enumerate() {
                    params_refs.clear();
                    params_refs
                        .extend(params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)));
                    if let Err(e) = transaction.execute(&stmt, &params_refs[..]).await {
                        warn!("BatchParams row {}/{} failed: {:?}", idx + 1, total, e);
                        failed += 1;
                    }
                }

                transaction.commit().await?;

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

    /// Execute a read operation against DuckDB.
    #[cfg(feature = "duckdb")]
    pub async fn execute_read<T, F>(
        &self,
        op: ReadOp<'_, T, F>,
    ) -> Result<ReadResult<T>, DbkitError>
    where
        F: Fn(&duckdb::Row<'_>) -> Result<T, DbkitError> + Send + 'static,
        T: Send + 'static,
    {
        let duck_conn = self
            .duck_conn
            .as_ref()
            .ok_or(DbkitError::DuckDbNotInitialized)?
            .clone();

        match op {
            ReadOp::Standard {
                query,
                params,
                map_fn,
                mode,
            } => {
                let query = query.to_string();
                let params = params.clone();

                let results = task::spawn_blocking(move || {
                    let conn = duck_conn
                        .lock()
                        .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;
                    let mut stmt = conn
                        .prepare(&query)
                        .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

                    let duck_values = Self::convert_params(&params);
                    let param_refs: Vec<&dyn duckdb::ToSql> =
                        duck_values.iter().map(|v| v as &dyn duckdb::ToSql).collect();

                    let rows = stmt
                        .query_map(param_refs.as_slice(), |row| {
                            map_fn(row).map_err(|e| {
                                duckdb::Error::InvalidParameterName(e.to_string())
                            })
                        })
                        .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

                    let mut results = Vec::new();
                    for row in rows {
                        results
                            .push(row.map_err(|e| DbkitError::DuckDb(e.to_string()))?);
                    }
                    Ok::<Vec<T>, DbkitError>(results)
                })
                .await
                .map_err(|e| DbkitError::TaskJoin(e.to_string()))??;

                let query_result = match mode {
                    FetchMode::None => QueryResult::None,
                    FetchMode::One => {
                        if results.len() != 1 {
                            return Err(DbkitError::RowCount {
                                expected: "1".into(),
                                actual: results.len(),
                            });
                        }
                        QueryResult::One(results.into_iter().next().unwrap())
                    }
                    FetchMode::Optional => {
                        if results.len() > 1 {
                            return Err(DbkitError::RowCount {
                                expected: "0 or 1".into(),
                                actual: results.len(),
                            });
                        }
                        QueryResult::Optional(results.into_iter().next())
                    }
                    FetchMode::All => QueryResult::All(results),
                };

                Ok(ReadResult::Standard(query_result))
            }
        }
    }

    // ==================== ARROW READ ====================

    /// Execute a query against DuckDB and return results as Arrow RecordBatches.
    #[cfg(feature = "duckdb")]
    pub async fn execute_arrow(
        &self,
        query: &str,
        params: &[DuckParam],
    ) -> Result<Vec<RecordBatch>, DbkitError> {
        let duck_conn = self
            .duck_conn
            .as_ref()
            .ok_or(DbkitError::DuckDbNotInitialized)?
            .clone();

        let query = query.to_string();
        let params = params.to_vec();

        task::spawn_blocking(move || {
            let conn = duck_conn
                .lock()
                .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;
            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

            let duck_values = Self::convert_params(&params);
            let param_refs: Vec<&dyn duckdb::ToSql> =
                duck_values.iter().map(|v| v as &dyn duckdb::ToSql).collect();

            let arrow_iter = stmt
                .query_arrow(param_refs.as_slice())
                .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

            Ok(arrow_iter.collect())
        })
        .await
        .map_err(|e| DbkitError::TaskJoin(e.to_string()))?
    }

    // ==================== SYNC (PG -> DuckDB) ====================

    /// Copy entire tables from Postgres into DuckDB local memory for analytical reads.
    ///
    /// Creates `memory.main.{table}` for each table, replacing any existing copy.
    #[cfg(feature = "duckdb")]
    pub async fn sync_tables(&self, tables: &[&str]) -> Result<(), DbkitError> {
        let duck_conn = self
            .duck_conn
            .as_ref()
            .ok_or(DbkitError::DuckDbNotInitialized)?
            .clone();

        let tables: Vec<String> = tables.iter().map(|t| t.to_string()).collect();

        task::spawn_blocking(move || {
            let conn = duck_conn
                .lock()
                .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;

            for table in &tables {
                let sql = format!(
                    "CREATE OR REPLACE TABLE memory.main.{table} AS SELECT * FROM pg.public.{table}"
                );
                conn.execute(&sql, [])
                    .map_err(|e| DbkitError::DuckDb(format!("sync {table}: {e}")))?;
            }
            Ok(())
        })
        .await
        .map_err(|e| DbkitError::TaskJoin(e.to_string()))?
    }

    /// Copy a filtered subset of a Postgres table into DuckDB local memory.
    ///
    /// The `filter` is a SQL WHERE clause (without the `WHERE` keyword).
    /// Creates `memory.main.{table}`, replacing any existing copy.
    #[cfg(feature = "duckdb")]
    pub async fn sync_table_filtered(
        &self,
        table: &str,
        filter: &str,
        params: &[DuckParam],
    ) -> Result<(), DbkitError> {
        let duck_conn = self
            .duck_conn
            .as_ref()
            .ok_or(DbkitError::DuckDbNotInitialized)?
            .clone();

        let table = table.to_string();
        let filter = filter.to_string();
        let params = params.to_vec();

        task::spawn_blocking(move || {
            let conn = duck_conn
                .lock()
                .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;

            let sql = format!(
                "CREATE OR REPLACE TABLE memory.main.{table} AS SELECT * FROM pg.public.{table} WHERE {filter}"
            );

            let duck_values = Self::convert_params(&params);
            let param_refs: Vec<&dyn duckdb::ToSql> =
                duck_values.iter().map(|v| v as &dyn duckdb::ToSql).collect();

            conn.execute(&sql, param_refs.as_slice())
                .map_err(|e| DbkitError::DuckDb(format!("sync_filtered {table}: {e}")))?;

            Ok(())
        })
        .await
        .map_err(|e| DbkitError::TaskJoin(e.to_string()))?
    }

    // ==================== PARAM CONVERSION ====================

    #[cfg(feature = "duckdb")]
    fn convert_params(params: &[DuckParam]) -> Vec<duckdb::types::Value> {
        params
            .iter()
            .map(|p| match p {
                DuckParam::Int(v) => duckdb::types::Value::Int(*v),
                DuckParam::Int64(v) => duckdb::types::Value::BigInt(*v),
                DuckParam::Float(v) => duckdb::types::Value::Double(*v),
                DuckParam::Text(v) => duckdb::types::Value::Text(v.clone()),
                DuckParam::Bool(v) => duckdb::types::Value::Boolean(*v),
                DuckParam::Null => duckdb::types::Value::Null,
                DuckParam::OptInt(v) => match v {
                    Some(val) => duckdb::types::Value::Int(*val),
                    None => duckdb::types::Value::Null,
                },
                DuckParam::OptInt64(v) => match v {
                    Some(val) => duckdb::types::Value::BigInt(*val),
                    None => duckdb::types::Value::Null,
                },
                DuckParam::OptFloat(v) => match v {
                    Some(val) => duckdb::types::Value::Double(*val),
                    None => duckdb::types::Value::Null,
                },
                DuckParam::OptText(v) => match v {
                    Some(val) => duckdb::types::Value::Text(val.clone()),
                    None => duckdb::types::Value::Null,
                },
                DuckParam::OptBool(v) => match v {
                    Some(val) => duckdb::types::Value::Boolean(*val),
                    None => duckdb::types::Value::Null,
                },
            })
            .collect()
    }
}
