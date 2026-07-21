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
    /// Same query executed once per parameter set, in a single transaction.
    ///
    /// Use for batched `INSERT … ON CONFLICT`, `UPDATE`s, or any non-Postgres
    /// backend. For a plain high-volume insert into one table,
    /// [`PgHandler::copy_in`](crate::PgHandler::copy_in) is ~30–50× faster — see
    /// its docs for a full `copy_in`-vs-`BatchParams` decision guide.
    BatchParams {
        query: &'a str,
        params_list: Vec<Vec<DbValue>>,
        /// Per-row error isolation.
        ///
        /// - `true` — a bad row is contained and the rest of the batch still
        ///   commits. Both [`PgHandler`](crate::PgHandler) and the
        ///   multi-backend [`BaseHandler`] wrap each row in a `SAVEPOINT`
        ///   (standard SQL: Postgres, MySQL/InnoDB, SQLite), so a failed row
        ///   rolls back alone instead of aborting the transaction.
        /// - `false` — **all-or-nothing**: no per-row savepoints, so the first
        ///   error rolls back the whole batch. ~2× faster than the isolated
        ///   path. Use for trusted bulk inserts where partial success isn't
        ///   needed. For the fastest plain bulk load, prefer
        ///   [`PgHandler::copy_in`](crate::PgHandler::copy_in).
        isolate_rows: bool,
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
            // A text-typed NULL, matching the text fallback used for the rich
            // variants below, so a nullable column behaves the same whether a
            // given row's value is NULL or not. (Binding `Option::<i64>::None`
            // — as dbkit < 0.5 did — declared the parameter as `int8` on
            // Postgres, so NULLs into varchar/date/json columns failed with
            // "column is of type X but expression is of type bigint".) For
            // non-text Postgres columns, cast explicitly in SQL (`$1::date`) —
            // the same rule as the rich-type text fallback. For native typed
            // NULL inference use `PgHandler`.
            DbValue::Null => q.bind(Option::<String>::None),
            DbValue::Bool(b) => q.bind(*b),
            DbValue::Int(i) => q.bind(*i),
            DbValue::Float(f) => q.bind(*f),
            DbValue::Text(s) => q.bind(s.clone()),
            DbValue::Bytes(b) => q.bind(b.clone()),
            // The Any driver can't carry native temporal/json/uuid types, so
            // bind a text rendering — Postgres assignment casts handle the rest.
            // For native rich-typed binds use `PgHandler` instead.
            #[cfg(feature = "postgres-native")]
            DbValue::Date(d) => q.bind(d.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::DateTime(dt) => q.bind(dt.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::TimestampTz(dt) => q.bind(dt.to_rfc3339()),
            #[cfg(feature = "postgres-native")]
            DbValue::Json(j) => q.bind(j.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::Uuid(u) => q.bind(u.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::Time(t) => q.bind(t.to_string()),
            #[cfg(feature = "postgres-native")]
            DbValue::TextArray(v) => q.bind(crate::value::pg_text_array_literal(v)),
            #[cfg(feature = "postgres-native")]
            DbValue::FloatArray(v) => {
                q.bind(crate::value::pg_float_array_literal(v.iter().map(|x| Some(*x))))
            }
            #[cfg(feature = "postgres-native")]
            DbValue::OptFloatArray(v) => {
                q.bind(crate::value::pg_float_array_literal(v.iter().copied()))
            }
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

    /// Like [`Self::with_duckdb_attached_postgres`], but the attached Postgres
    /// tables also resolve **unqualified** — `FROM users` finds
    /// `pg.public.users`.
    ///
    /// Uses the `memory.main,pg.public` search path, so `sync_*` tables are
    /// searched first and keep shadowing Postgres; only names that would
    /// otherwise error now reach Postgres.
    #[cfg(feature = "duckdb")]
    pub fn with_duckdb_attached_postgres_searchable(
        pool: AnyPool,
        pg_connection_string: &str,
    ) -> Result<Self, DbkitError> {
        let engine = crate::read::duckdb::DuckEngine::new_in_memory()?;
        engine.attach_postgres_searchable(pg_connection_string)?;
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

    /// Accent-insensitive name key: NFD-decompose, DROP combining marks, then
    /// lowercase — "José Ramírez" and "Jose Ramirez" produce the same key.
    ///
    /// NFD alone only equalizes composed vs decomposed representations of the
    /// SAME accented string; the combining marks survive, so an accented name
    /// never matched its accent-stripped variant (a common shape across data
    /// feeds — US sources routinely strip diacritics). Stripping the marks
    /// after decomposition makes the comparison genuinely accent-insensitive.
    pub fn normalize_name(name: &str) -> String {
        use unicode_normalization::char::is_combining_mark;
        name.nfd()
            .filter(|c| !is_combining_mark(*c))
            .collect::<String>()
            .to_lowercase()
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
                // Statement-reuse guard (same as PgHandler): sqlx caches one
                // prepared statement per (connection, SQL), pinning parameter
                // types from the first execution. A NULL here binds as text,
                // so a call whose NULL/concrete pattern differs from the
                // cached statement's types would fail (22P03 / type mismatch)
                // on Postgres. Re-parse NULL-bearing calls; keep caching for
                // the common no-NULL case.
                let has_null = params.iter().any(|v| matches!(v, DbValue::Null));
                let q = bind_params(sqlx::query(AssertSqlSafe(query)), &params)
                    .persistent(!has_null);
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
                isolate_rows,
            } => {
                if params_list.is_empty() {
                    return Ok(QueryResult::None);
                }

                let total = params_list.len();
                let mut tx = self.pool.begin().await?;

                if !isolate_rows {
                    // All-or-nothing fast path: the first error aborts the whole
                    // batch (propagated below). No per-row bookkeeping.
                    //
                    // Statement reuse: re-parse NULL-bearing rows so their param
                    // types don't collide with the cached statement's pinned
                    // types (NULL binds as text; see `bind_params`).
                    for params in &params_list {
                        let has_null = params.iter().any(|v| matches!(v, DbValue::Null));
                        bind_params(sqlx::query(AssertSqlSafe(query)), params)
                            .persistent(!has_null)
                            .execute(&mut *tx)
                            .await?;
                    }
                    tx.commit().await?;
                    return Ok(QueryResult::None);
                }

                let mut failed = 0usize;
                for (idx, params) in params_list.iter().enumerate() {
                    // Wrap each row in a SAVEPOINT (standard SQL — Postgres,
                    // MySQL/InnoDB, and SQLite all support it) so a bad row
                    // rolls back on its own instead of poisoning the batch.
                    // Postgres in particular marks the whole transaction failed
                    // on the first error without this: every following row died
                    // with 25P02 and the final COMMIT silently became ROLLBACK,
                    // so one bad row used to lose the entire batch (dbkit < 0.5)
                    // while still returning Ok.
                    sqlx::query(AssertSqlSafe("SAVEPOINT dbkit_row"))
                        .execute(&mut *tx)
                        .await?;
                    let has_null = params.iter().any(|v| matches!(v, DbValue::Null));
                    let q = bind_params(sqlx::query(AssertSqlSafe(query)), params)
                        .persistent(!has_null);
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
    ///
    /// An **empty result drops the analytical table** (the schema can't be
    /// inferred from zero rows): reads of a table synced empty error with
    /// "table not found" rather than silently serving rows from a previous
    /// sync.
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

        match crate::read::rows_to_record_batch(&rows)? {
            Some(batch) => engine.load_table(name, vec![batch]).await?,
            None => engine.drop_table(name).await?,
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
