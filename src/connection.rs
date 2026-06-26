use crate::config::{Backend, DbkitConfig};
use crate::DbkitError;
use sqlx::migrate::MigrateDatabase;
use sqlx::{Any, AnyPool, any::AnyPoolOptions};
use std::time::Duration;
use tracing::{info, warn};

/// Multi-backend connection pool with optional automatic database creation.
///
/// The backend (Postgres, MySQL, or SQLite) is selected from the URL scheme.
/// If `auto_create_db` is enabled (default) and the target database doesn't
/// exist, it is created before the pool is opened.
pub struct ConnectionManager {
    pool: AnyPool,
    backend: Backend,
    db_name: String,
    connection_string: String,
    config: DbkitConfig,
}

impl ConnectionManager {
    /// Connect using a [`DbkitConfig`].
    pub async fn connect(config: DbkitConfig) -> Result<Self, DbkitError> {
        // Registers the Postgres/MySQL/SQLite drivers compiled in via features.
        sqlx::any::install_default_drivers();

        let backend = Backend::from_url(&config.url)?;
        let db_name = Self::extract_db_name(&config.url, backend);
        let connection_string = config.url.clone();

        if config.auto_create_db {
            Self::ensure_database(&config.url, &db_name).await?;
        }

        let pool = Self::build_pool(&config)
            .await
            .map_err(|e| Self::map_connect_error(e, &db_name))?;

        info!("connected to database '{}' ({:?})", db_name, backend);

        Ok(Self {
            pool,
            backend,
            db_name,
            connection_string,
            config,
        })
    }

    /// Connect using a connection URL with default settings.
    ///
    /// Shorthand for `ConnectionManager::connect(DbkitConfig::from_url(url))`.
    pub async fn new(url: &str) -> Result<Self, DbkitError> {
        Self::connect(DbkitConfig::from_url(url)).await
    }

    /// Get the underlying sqlx connection pool.
    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    /// The detected backend for this connection.
    pub fn backend(&self) -> Backend {
        self.backend
    }

    /// Acquire a connection from the pool.
    pub async fn get_connection(&self) -> Result<sqlx::pool::PoolConnection<Any>, DbkitError> {
        self.pool
            .acquire()
            .await
            .map_err(|e| DbkitError::Pool(e.to_string()))
    }

    /// Check if the database is reachable.
    pub async fn is_connected(&self) -> bool {
        self.pool.acquire().await.is_ok()
    }

    /// The database name extracted from the connection URL.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// The full connection string.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// The config used to create this connection.
    pub fn config(&self) -> &DbkitConfig {
        &self.config
    }

    /// Create a native sqlx [`PgPool`](sqlx::PgPool) from this connection's URL.
    ///
    /// The multi-backend `Any` pool can only represent basic scalar types
    /// (bool/int/float/text/bytes). This native pool restores full Postgres type
    /// support — `uuid`, `chrono` timestamps, `json`/`jsonb`, arrays, etc. — for
    /// the queries that need it. Use it alongside [`pool`](Self::pool): `Any` for
    /// portable work, the native pool for rich-typed Postgres work.
    ///
    /// Errors if this connection is not Postgres.
    ///
    /// ```ignore
    /// let pg = conn.pg_native_pool().await?;
    /// let row = sqlx::query("SELECT id, created_at FROM users WHERE id = $1")
    ///     .bind(some_uuid)
    ///     .fetch_one(&pg)
    ///     .await?;
    /// let id: sqlx::types::Uuid = row.get("id");
    /// ```
    #[cfg(feature = "postgres-native")]
    pub async fn pg_native_pool(&self) -> Result<sqlx::PgPool, DbkitError> {
        if self.backend != Backend::Postgres {
            return Err(DbkitError::UnsupportedBackend(format!(
                "pg_native_pool requires a Postgres connection, got {:?}",
                self.backend
            )));
        }
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(self.config.pool_size as u32)
            .acquire_timeout(Duration::from_secs(self.config.connect_timeout_secs))
            .idle_timeout(Duration::from_secs(self.config.idle_timeout_secs))
            .connect(&self.connection_string)
            .await
            .map_err(|e| DbkitError::Pool(e.to_string()))
    }

    /// Pool health metrics.
    pub fn pool_status(&self) -> PoolStatus {
        PoolStatus {
            max_size: self.pool.options().get_max_connections() as usize,
            size: self.pool.size() as usize,
            idle: self.pool.num_idle(),
        }
    }

    async fn build_pool(config: &DbkitConfig) -> Result<AnyPool, sqlx::Error> {
        AnyPoolOptions::new()
            .max_connections(config.pool_size as u32)
            .acquire_timeout(Duration::from_secs(config.connect_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .connect(&config.url)
            .await
    }

    /// Create the database if it does not already exist.
    ///
    /// Uses sqlx's backend-agnostic `MigrateDatabase`, which handles
    /// `CREATE DATABASE` for Postgres/MySQL and file creation for SQLite.
    async fn ensure_database(url: &str, db_name: &str) -> Result<(), DbkitError> {
        let exists = Any::database_exists(url).await.map_err(|e| {
            DbkitError::DatabaseCreation {
                name: db_name.to_string(),
                reason: e.to_string(),
            }
        })?;

        if !exists {
            warn!("database '{}' does not exist, creating...", db_name);
            Any::create_database(url)
                .await
                .map_err(|e| DbkitError::DatabaseCreation {
                    name: db_name.to_string(),
                    reason: e.to_string(),
                })?;
            info!("database '{}' created", db_name);
        }

        Ok(())
    }

    /// Map a sqlx connection error onto a more specific [`DbkitError`] where the
    /// backend exposes a recognizable SQLSTATE.
    fn map_connect_error(e: sqlx::Error, db_name: &str) -> DbkitError {
        if let sqlx::Error::Database(ref db) = e {
            match db.code().as_deref() {
                // Postgres: invalid_password / invalid_authorization_specification
                Some("28P01") | Some("28000") => return DbkitError::AuthFailed,
                // Postgres: too_many_connections
                Some("53300") => return DbkitError::TooManyConnections,
                _ => {}
            }
        }
        DbkitError::Connection(format!("could not connect to '{}': {}", db_name, e))
    }

    fn extract_db_name(url: &str, backend: Backend) -> String {
        match backend {
            // sqlite://path/to/file.db  ->  path/to/file.db
            Backend::Sqlite => url
                .splitn(2, ':')
                .nth(1)
                .unwrap_or("")
                .trim_start_matches("//")
                .split('?')
                .next()
                .unwrap_or("")
                .to_string(),
            // ...://host:port/dbname?params  ->  dbname
            _ => url
                .rsplit('/')
                .next()
                .unwrap_or("")
                .split('?')
                .next()
                .unwrap_or("")
                .to_string(),
        }
    }
}

/// Snapshot of connection pool health.
#[derive(Debug, Clone)]
pub struct PoolStatus {
    /// Maximum number of connections in the pool.
    pub max_size: usize,
    /// Current number of connections (active + idle).
    pub size: usize,
    /// Number of idle connections available.
    pub idle: usize,
}

impl std::fmt::Display for PoolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pool: {}/{} connections, {} idle",
            self.size, self.max_size, self.idle
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_db_name_server() {
        assert_eq!(
            ConnectionManager::extract_db_name("postgres://u:p@host:5432/myapp", Backend::Postgres),
            "myapp"
        );
        assert_eq!(
            ConnectionManager::extract_db_name(
                "mysql://host:3306/app?ssl-mode=required",
                Backend::MySql
            ),
            "app"
        );
    }

    #[test]
    fn extract_db_name_sqlite() {
        assert_eq!(
            ConnectionManager::extract_db_name("sqlite://data/app.db", Backend::Sqlite),
            "data/app.db"
        );
    }
}
