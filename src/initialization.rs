use crate::base_handler::{BaseHandler, FetchMode, WriteOp};
use crate::DbkitError;
use deadpool_postgres::Pool;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::{error, info};

/// Batch DDL migration executor with tracking.
///
/// Maintains a `_dbkit_migrations` table so already-applied migrations
/// are skipped on subsequent runs. Each migration is identified by a
/// user-provided name and a content hash.
pub struct InitializationHandler {
    handler: BaseHandler,
}

impl InitializationHandler {
    pub fn new(pool: Arc<Pool>) -> Self {
        Self {
            handler: BaseHandler::new(pool),
        }
    }

    /// Ensure the migrations tracking table exists.
    async fn ensure_tracking_table(&self) -> Result<(), DbkitError> {
        self.handler
            .execute_write(WriteOp::BatchDDL {
                queries: &[
                    "CREATE TABLE IF NOT EXISTS _dbkit_migrations (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        hash TEXT NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )",
                ],
            })
            .await?;
        Ok(())
    }

    /// Compute a hash of the SQL content for change detection.
    fn hash_sql(sql: &str) -> String {
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Run a named migration. Skips if already applied with the same content hash.
    ///
    /// If the migration name exists but the hash differs, it returns an error
    /// (content changed after being applied).
    pub async fn run_named_migration(&self, name: &str, sql: &str) -> Result<(), DbkitError> {
        self.ensure_tracking_table().await?;

        let hash = Self::hash_sql(sql);

        // Check if already applied
        let result = self
            .handler
            .execute_write(WriteOp::Single {
                query: "SELECT hash FROM _dbkit_migrations WHERE name = $1",
                params: &[&name],
                mode: FetchMode::Optional,
            })
            .await?;

        if let Some(row) = result.optional()? {
            let existing_hash: String = row.get(0);
            if existing_hash == hash {
                info!("migration '{}' already applied, skipping", name);
                return Ok(());
            } else {
                return Err(DbkitError::Migration(format!(
                    "migration '{}' was already applied but content has changed (hash {} → {})",
                    name, existing_hash, hash
                )));
            }
        }

        // Run the migration
        info!("applying migration '{}'...", name);
        let queries: Vec<String> = sql
            .split(';')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let query_refs: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();

        match self
            .handler
            .execute_write(WriteOp::BatchDDL {
                queries: &query_refs,
            })
            .await
        {
            Ok(_) => {
                info!(
                    "migration '{}': {} DDL statements executed",
                    name,
                    query_refs.len()
                );
            }
            Err(e) => {
                error!("migration '{}' failed: {:?}", name, e);
                return Err(DbkitError::Migration(e.to_string()));
            }
        }

        // Record the migration
        self.handler
            .execute_write(WriteOp::Single {
                query: "INSERT INTO _dbkit_migrations (name, hash) VALUES ($1, $2)",
                params: &[&name, &hash],
                mode: FetchMode::None,
            })
            .await?;

        info!("migration '{}' recorded", name);
        Ok(())
    }

    /// Run migrations from a SQL string (semicolon-separated DDL statements).
    ///
    /// This is the simple/legacy API — it runs all statements unconditionally
    /// without tracking. Use [`run_named_migration`] for tracked migrations.
    pub async fn run_migrations(&self, sql: &str) -> Result<(), DbkitError> {
        info!("running database migrations...");

        let queries: Vec<String> = sql
            .split(';')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let query_refs: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();

        match self
            .handler
            .execute_write(WriteOp::BatchDDL {
                queries: &query_refs,
            })
            .await
        {
            Ok(_) => {
                info!("{} DDL statements executed", query_refs.len());
            }
            Err(e) => {
                error!("migration failed: {:?}", e);
                return Err(DbkitError::Migration(e.to_string()));
            }
        }

        Ok(())
    }

    /// List all applied migrations (name, hash, applied_at).
    pub async fn applied_migrations(&self) -> Result<Vec<(String, String, String)>, DbkitError> {
        self.ensure_tracking_table().await?;

        let result = self
            .handler
            .execute_write(WriteOp::Single {
                query: "SELECT name, hash, applied_at::TEXT FROM _dbkit_migrations ORDER BY id",
                params: &[],
                mode: FetchMode::All,
            })
            .await?;

        let rows = result.all()?;
        Ok(rows
            .iter()
            .map(|row| {
                let name: String = row.get(0);
                let hash: String = row.get(1);
                let applied_at: String = row.get(2);
                (name, hash, applied_at)
            })
            .collect())
    }
}
