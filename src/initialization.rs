use crate::DbkitError;
use crate::base_handler::{BaseHandler, FetchMode, WriteOp};
use crate::config::Backend;
use sqlx::{AnyPool, Row};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::{error, info};

/// Batch DDL migration executor with tracking.
///
/// Maintains a `_dbkit_migrations` table so already-applied migrations
/// are skipped on subsequent runs. Each migration is identified by a
/// user-provided name and a content hash.
///
/// The tracking table DDL and the internal queries are backend-aware
/// (Postgres / MySQL / SQLite), so the migration runner works on any backend.
/// The *user-supplied* migration SQL is still backend-native — write it for the
/// database you connected to.
pub struct InitializationHandler {
    handler: BaseHandler,
    backend: Backend,
}

impl InitializationHandler {
    /// Create a handler for the given pool and backend.
    ///
    /// The backend is typically obtained from
    /// [`ConnectionManager::backend`](crate::ConnectionManager::backend).
    pub fn new(pool: AnyPool, backend: Backend) -> Self {
        Self {
            handler: BaseHandler::new(pool),
            backend,
        }
    }

    /// The placeholder for the `n`-th bind parameter (1-based) for this backend.
    fn placeholder(&self, n: usize) -> String {
        match self.backend {
            Backend::Postgres => format!("${n}"),
            Backend::MySql | Backend::Sqlite => "?".to_string(),
        }
    }

    /// Backend-specific DDL for the migrations tracking table.
    ///
    /// `name` is the primary key (no auto-increment column needed), which keeps
    /// the schema portable. `applied_at` is a timestamp defaulting to "now".
    fn tracking_table_ddl(&self) -> &'static str {
        match self.backend {
            Backend::Postgres => {
                "CREATE TABLE IF NOT EXISTS _dbkit_migrations (
                    name TEXT PRIMARY KEY,
                    hash TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )"
            }
            Backend::MySql => {
                // TEXT can't be a primary key without a prefix length in MySQL.
                "CREATE TABLE IF NOT EXISTS _dbkit_migrations (
                    name VARCHAR(255) PRIMARY KEY,
                    hash TEXT NOT NULL,
                    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"
            }
            Backend::Sqlite => {
                "CREATE TABLE IF NOT EXISTS _dbkit_migrations (
                    name TEXT PRIMARY KEY,
                    hash TEXT NOT NULL,
                    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"
            }
        }
    }

    /// A SQL expression that renders `applied_at` as text, so it can be read
    /// through sqlx's `Any` driver (which can't decode native timestamp types).
    fn applied_at_as_text(&self) -> &'static str {
        match self.backend {
            // MySQL casts to CHAR; Postgres and SQLite cast to TEXT.
            Backend::MySql => "CAST(applied_at AS CHAR)",
            Backend::Postgres | Backend::Sqlite => "CAST(applied_at AS TEXT)",
        }
    }

    /// Ensure the migrations tracking table exists.
    async fn ensure_tracking_table(&self) -> Result<(), DbkitError> {
        self.handler
            .execute_write(WriteOp::BatchDDL {
                queries: &[self.tracking_table_ddl()],
            })
            .await?;
        Ok(())
    }

    /// Compute a hash of the SQL content for change detection.
    ///
    /// FNV-1a (64-bit), implemented inline: the algorithm is fixed forever, so
    /// hashes persisted in `_dbkit_migrations` stay comparable across dbkit and
    /// Rust releases. (`DefaultHasher` — used before 0.5 — explicitly does not
    /// guarantee a stable algorithm between Rust releases, which would have
    /// made every already-applied migration report "content has changed" after
    /// a toolchain upgrade.)
    fn hash_sql(sql: &str) -> String {
        const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut h = FNV_OFFSET;
        for b in sql.as_bytes() {
            h ^= u64::from(*b);
            h = h.wrapping_mul(FNV_PRIME);
        }
        format!("{h:016x}")
    }

    /// The pre-0.5 hash (`DefaultHasher`), kept only to recognize rows recorded
    /// by older dbkit versions so they can be upgraded in place rather than
    /// misreported as changed content.
    fn legacy_hash_sql(sql: &str) -> String {
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
        let select = format!(
            "SELECT hash FROM _dbkit_migrations WHERE name = {}",
            self.placeholder(1)
        );
        let result = self
            .handler
            .execute_write(WriteOp::Single {
                query: select.as_str(),
                params: vec![name.into()],
                mode: FetchMode::Optional,
            })
            .await?;

        if let Some(row) = result.optional()? {
            let existing_hash: String = row.get(0);
            if existing_hash == hash {
                info!("migration '{}' already applied, skipping", name);
                return Ok(());
            }
            // A row recorded by dbkit < 0.5 holds a `DefaultHasher` hash. If it
            // matches the same SQL under the legacy algorithm, the content is
            // unchanged — upgrade the stored hash in place and skip.
            if existing_hash == Self::legacy_hash_sql(sql) {
                info!(
                    "migration '{}' already applied (pre-0.5 hash), upgrading stored hash",
                    name
                );
                let update = format!(
                    "UPDATE _dbkit_migrations SET hash = {} WHERE name = {}",
                    self.placeholder(1),
                    self.placeholder(2)
                );
                self.handler
                    .execute_write(WriteOp::Single {
                        query: update.as_str(),
                        params: vec![hash.into(), name.into()],
                        mode: FetchMode::None,
                    })
                    .await?;
                return Ok(());
            }
            return Err(DbkitError::Migration(format!(
                "migration '{}' was already applied but content has changed (hash {} → {})",
                name, existing_hash, hash
            )));
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
        let insert = format!(
            "INSERT INTO _dbkit_migrations (name, hash) VALUES ({}, {})",
            self.placeholder(1),
            self.placeholder(2)
        );
        self.handler
            .execute_write(WriteOp::Single {
                query: insert.as_str(),
                params: vec![name.into(), hash.clone().into()],
                mode: FetchMode::None,
            })
            .await?;

        info!("migration '{}' recorded", name);
        Ok(())
    }

    /// Run migrations from a SQL string (semicolon-separated DDL statements).
    ///
    /// Statements are split on `;`, so this does not support SQL that embeds
    /// semicolons inside a single statement (string literals, PL/pgSQL
    /// function bodies, triggers). Run such migrations as a single statement
    /// through [`WriteOp::BatchDDL`] directly.
    ///
    /// This is the simple/legacy API — it runs all statements unconditionally
    /// without tracking. Use [`run_named_migration`](Self::run_named_migration)
    /// for tracked migrations.
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

    /// List all applied migrations (name, hash, applied_at) in application order.
    pub async fn applied_migrations(&self) -> Result<Vec<(String, String, String)>, DbkitError> {
        self.ensure_tracking_table().await?;

        let select = format!(
            "SELECT name, hash, {} FROM _dbkit_migrations ORDER BY applied_at, name",
            self.applied_at_as_text()
        );
        let result = self
            .handler
            .execute_write(WriteOp::Single {
                query: select.as_str(),
                params: vec![],
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The stored migration hashes must never change meaning, so pin the
    /// algorithm to the published FNV-1a 64-bit reference vectors.
    #[test]
    fn hash_sql_is_stable_fnv1a() {
        assert_eq!(InitializationHandler::hash_sql(""), "cbf29ce484222325");
        assert_eq!(InitializationHandler::hash_sql("a"), "af63dc4c8601ec8c");
        assert_eq!(InitializationHandler::hash_sql("foobar"), "85944171f73967e8");
    }
}
