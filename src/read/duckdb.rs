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
    /// `search_path` to apply to every connection this engine hands out.
    ///
    /// DuckDB's `search_path` (like `USE`) is CONNECTION-scoped, and
    /// [`ReadEngine::query_arrow`] runs each query on its own `try_clone()`
    /// connection — so a path set once on the root connection is *gone* by the
    /// time a query executes. Storing it here lets every clone re-apply it.
    /// `None` (the default) leaves resolution exactly as DuckDB ships it.
    search_path: Arc<Mutex<Option<String>>>,
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
            search_path: Arc::new(Mutex::new(None)),
        })
    }

    /// Set the `search_path` applied to **every** connection this engine uses,
    /// so unqualified table names resolve as configured.
    ///
    /// Necessary because DuckDB scopes `search_path`/`USE` to a single
    /// connection while [`ReadEngine::query_arrow`] executes each query on a
    /// fresh `try_clone()`. Setting the path once on the root connection has no
    /// effect on later queries; this stores it so each clone re-applies it.
    ///
    /// Pass a comma-separated list, e.g. `"memory.main,pg.public"`.
    pub fn set_search_path(&self, search_path: impl Into<String>) -> Result<(), DbkitError> {
        let path = search_path.into();
        // Validate eagerly on the root connection so a bad path fails here
        // rather than on every subsequent query.
        {
            let conn = self
                .conn
                .lock()
                .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?;
            conn.execute(&format!("SET search_path='{}'", path.replace('\'', "''")), [])
                .map_err(|e| DbkitError::DuckDb(e.to_string()))?;
        }
        *self
            .search_path
            .lock()
            .map_err(|e| DbkitError::LockPoisoned(e.to_string()))? = Some(path);
        Ok(())
    }

    /// Attach a Postgres database so its tables can be queried live — without
    /// copying — through the `pg` catalog (e.g. `SELECT * FROM pg.public.users`).
    ///
    /// Runs `INSTALL postgres; LOAD postgres;` (which may download the DuckDB
    /// Postgres extension on first use) followed by `ATTACH`. This restores the
    /// pre-rewrite zero-copy `ATTACH` path; it does not change the default
    /// catalog, so synced in-memory tables are unaffected. Blocking; intended
    /// for one-time setup.
    ///
    /// Unqualified names still won't resolve to Postgres — use
    /// [`Self::attach_postgres_searchable`] (or [`Self::set_search_path`]) if
    /// you want `FROM users` to find `pg.public.users`.
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

    /// [`Self::attach_postgres`] plus a `search_path` that makes the attached
    /// Postgres tables resolve **unqualified** — `FROM users` finds
    /// `pg.public.users`.
    ///
    /// The path is `memory.main,pg.public`: the local in-memory catalog is
    /// searched FIRST, so `sync_*`/`load_table` tables keep shadowing Postgres
    /// and existing behaviour is preserved. Only names that would otherwise
    /// have errored now resolve to Postgres.
    ///
    /// Prefer this when the DuckDB instance exists to query one attached
    /// Postgres database — writing `pg.public.` in front of every table is
    /// noisy, and forgetting it produces a confusing
    /// `Table with name X does not exist! Did you mean "pg.X"?`.
    pub fn attach_postgres_searchable(&self, connection_string: &str) -> Result<(), DbkitError> {
        self.attach_postgres(connection_string)?;
        self.set_search_path("memory.main,pg.public")
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
        // `search_path` is CONNECTION-scoped, so the clone above does NOT
        // inherit one set on the root connection — it must be re-applied here
        // or unqualified names silently fail to resolve (e.g. an ATTACHed
        // Postgres table erroring with `Did you mean "pg.X"?`). Cheap: a
        // settings write on an in-process DB, no I/O.
        let search_path = self
            .search_path
            .lock()
            .map_err(|e| DbkitError::LockPoisoned(e.to_string()))?
            .clone();
        let sql = sql.to_string();
        let params = params.to_vec();

        task::spawn_blocking(move || {
            if let Some(path) = search_path {
                conn.execute(&format!("SET search_path='{}'", path.replace('\'', "''")), [])
                    .map_err(|e| DbkitError::DuckDb(e.to_string()))?;
            }
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

    /// `search_path` is CONNECTION-scoped, so a path set only on the root
    /// connection is lost when `query_arrow` clones — which silently broke
    /// unqualified reads against an ATTACHed catalog. Pin that
    /// `set_search_path` survives the clone.
    ///
    /// Uses a second in-memory catalog (ATTACH ':memory:') as a stand-in for
    /// the Postgres attachment so the test needs no live database.
    #[tokio::test]
    async fn search_path_survives_the_per_query_connection_clone() {
        let eng = DuckEngine::new_in_memory().unwrap();
        {
            let conn = eng.conn.lock().unwrap();
            conn.execute_batch(
                "ATTACH ':memory:' AS other; \
                 CREATE TABLE other.main.widgets AS SELECT * FROM range(3) r(a);",
            )
            .unwrap();
        }

        // Unqualified resolution fails before a search path is configured.
        assert!(
            eng.query_arrow("SELECT count(*) FROM widgets", &[])
                .await
                .is_err(),
            "unqualified name must not resolve without a search_path"
        );

        eng.set_search_path("memory.main,other.main").unwrap();

        let batches = eng
            .query_arrow("SELECT count(*) AS n FROM widgets", &[])
            .await
            .expect("search_path must be re-applied on the cloned connection");
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3);

        // Local catalog is searched FIRST, so a same-named local table still
        // shadows the attached one (sync_*/load_table behaviour preserved).
        {
            let conn = eng.conn.lock().unwrap();
            conn.execute_batch("CREATE TABLE widgets AS SELECT * FROM range(99) r(a)")
                .unwrap();
        }
        let batches = eng
            .query_arrow("SELECT count(*) AS n FROM widgets", &[])
            .await
            .unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 99, "local catalog must shadow the attached one");
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
