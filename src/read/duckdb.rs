//! DuckDB analytical read engine.

use crate::DbkitError;
use crate::analytical::RecordBatch;
use crate::read::ReadEngine;
use crate::value::DbValue;
use ::duckdb::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::task;

/// An in-memory DuckDB instance used for analytical reads.
///
/// DuckDB is synchronous, so queries run on a blocking thread pool. The
/// connection is shared behind a `Mutex` since a single DuckDB connection is
/// not `Sync`.
pub struct DuckEngine {
    conn: Arc<Mutex<::duckdb::Connection>>,
}

impl DuckEngine {
    /// Open a fresh in-memory DuckDB instance.
    pub fn new_in_memory() -> Result<Self, DbkitError> {
        let conn = ::duckdb::Connection::open_in_memory()
            .map_err(|e| DbkitError::DuckDb(e.to_string()))?;
        // Register the Arrow virtual table so batches can be ingested via
        // `SELECT * FROM arrow(?, ?)`.
        conn.register_table_function::<ArrowVTab>("arrow")
            .map_err(|e| DbkitError::DuckDb(e.to_string()))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Attach a Postgres database so its tables can be queried live — without
    /// copying — through the `pg` catalog (e.g. `SELECT * FROM pg.public.users`).
    ///
    /// Runs `INSTALL postgres; LOAD postgres;` (which may download the DuckDB
    /// Postgres extension on first use) followed by `ATTACH`. This restores the
    /// pre-rewrite zero-copy `ATTACH` path; it does not change the default
    /// catalog, so synced in-memory tables are unaffected. Blocking; intended
    /// for one-time setup.
    pub fn attach_postgres(&self, connection_string: &str) -> Result<(), DbkitError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;
        conn.execute_batch("INSTALL postgres; LOAD postgres;")
            .map_err(|e| DbkitError::DuckDb(e.to_string()))?;
        conn.execute(
            &format!("ATTACH '{connection_string}' AS pg (TYPE POSTGRES)"),
            [],
        )
        .map_err(|e| DbkitError::DuckDb(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl ReadEngine for DuckEngine {
    async fn query_arrow(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<Vec<RecordBatch>, DbkitError> {
        let conn = self.conn.clone();
        let sql = sql.to_string();
        let params = params.to_vec();

        task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| DbkitError::DuckDb(e.to_string()))?;

            let values = convert_params(&params);
            let param_refs: Vec<&dyn ::duckdb::ToSql> =
                values.iter().map(|v| v as &dyn ::duckdb::ToSql).collect();

            let batches = stmt
                .query_arrow(param_refs.as_slice())
                .map_err(|e| DbkitError::DuckDb(e.to_string()))?
                .collect();

            Ok::<Vec<RecordBatch>, DbkitError>(batches)
        })
        .await
        .map_err(|e| DbkitError::TaskJoin(e.to_string()))?
    }

    async fn load_table(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DbkitError> {
        if batches.is_empty() {
            return Ok(());
        }

        let conn = self.conn.clone();
        let name = name.to_string();

        task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;

            for (i, batch) in batches.into_iter().enumerate() {
                // The first batch creates (or replaces) the table; the rest
                // append. DuckDB infers the schema from the Arrow data.
                let sql = if i == 0 {
                    format!("CREATE OR REPLACE TABLE \"{name}\" AS SELECT * FROM arrow(?, ?)")
                } else {
                    format!("INSERT INTO \"{name}\" SELECT * FROM arrow(?, ?)")
                };
                let params = arrow_recordbatch_to_query_params(batch);
                conn.execute(&sql, params)
                    .map_err(|e| DbkitError::DuckDb(format!("load_table {name}: {e}")))?;
            }
            Ok(())
        })
        .await
        .map_err(|e| DbkitError::TaskJoin(e.to_string()))?
    }
}

/// Convert dbkit's neutral [`DbValue`]s into DuckDB parameter values.
fn convert_params(params: &[DbValue]) -> Vec<::duckdb::types::Value> {
    use ::duckdb::types::Value;
    params
        .iter()
        .map(|p| match p {
            DbValue::Null => Value::Null,
            DbValue::Bool(b) => Value::Boolean(*b),
            DbValue::Int(i) => Value::BigInt(*i),
            DbValue::Float(f) => Value::Double(*f),
            DbValue::Text(s) => Value::Text(s.clone()),
            DbValue::Bytes(b) => Value::Blob(b.clone()),
        })
        .collect()
}
