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
        // Escape embedded single quotes (common in passwords) so the
        // connection string can't break out of the ATTACH literal.
        let escaped = connection_string.replace('\'', "''");
        conn.execute(&format!("ATTACH '{escaped}' AS pg (TYPE POSTGRES)"), [])
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
        // Run each query on its own cloned connection handle: `try_clone` is a
        // cheap second connection to the SAME DuckDB instance (shared catalog,
        // including synced tables and ATTACHed databases), so concurrent reads
        // execute in parallel instead of serializing behind the root
        // connection's Mutex. The lock is held only for the clone itself.
        let conn = {
            let guard = self
                .conn
                .lock()
                .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;
            guard
                .try_clone()
                .map_err(|e| DbkitError::DuckDb(e.to_string()))?
        };
        let sql = sql.to_string();
        let params = params.to_vec();

        task::spawn_blocking(move || {
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
                let quoted = quote_ident(&name);
                let sql = if i == 0 {
                    format!("CREATE OR REPLACE TABLE {quoted} AS SELECT * FROM arrow(?, ?)")
                } else {
                    format!("INSERT INTO {quoted} SELECT * FROM arrow(?, ?)")
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

    async fn drop_table(&self, name: &str) -> Result<(), DbkitError> {
        let conn = self.conn.clone();
        let name = name.to_string();

        task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;
            conn.execute(&format!("DROP TABLE IF EXISTS {}", quote_ident(&name)), [])
                .map_err(|e| DbkitError::DuckDb(format!("drop_table {name}: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| DbkitError::TaskJoin(e.to_string()))?
    }
}

/// Double-quote an identifier for DuckDB, escaping embedded quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytical::arrow::array::Int64Array;

    /// `query_arrow` runs on a `try_clone`d connection — prove the clone shares
    /// the root connection's catalog (tables created via the root are visible),
    /// and that `drop_table` really removes them.
    #[tokio::test]
    async fn cloned_connection_shares_catalog_and_drop_table_works() {
        let eng = DuckEngine::new_in_memory().unwrap();
        {
            let conn = eng.conn.lock().unwrap();
            conn.execute_batch("CREATE TABLE t AS SELECT * FROM range(5) r(a)")
                .unwrap();
        }

        let batches = eng
            .query_arrow("SELECT count(*) AS n FROM t WHERE a >= ?", &[DbValue::Int(1)])
            .await
            .expect("clone sees root's table");
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 4);

        eng.drop_table("t").await.unwrap();
        assert!(
            eng.query_arrow("SELECT * FROM t", &[]).await.is_err(),
            "table gone after drop_table"
        );
        // Dropping a never-loaded table is a no-op, not an error.
        eng.drop_table("never_loaded").await.unwrap();
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
            // Rich types are passed as text; DuckDB casts them against the
            // target column (incl. ATTACHed Postgres date/json columns).
            #[cfg(feature = "postgres-native")]
            DbValue::Date(d) => Value::Text(d.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::DateTime(dt) => Value::Text(dt.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::TimestampTz(dt) => Value::Text(dt.to_rfc3339()),
            #[cfg(feature = "postgres-native")]
            DbValue::Json(j) => Value::Text(j.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::Uuid(u) => Value::Text(u.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::Time(t) => Value::Text(t.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::TextArray(v) => Value::Text(crate::value::pg_text_array_literal(v)),
            #[cfg(feature = "postgres-native")]
            DbValue::FloatArray(v) => {
                Value::Text(crate::value::pg_float_array_literal(v.iter().map(|x| Some(*x))))
            }
            #[cfg(feature = "postgres-native")]
            DbValue::OptFloatArray(v) => {
                Value::Text(crate::value::pg_float_array_literal(v.iter().copied()))
            }
        })
        .collect()
}
