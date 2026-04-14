use crate::config::DbkitConfig;
use crate::DbkitError;
use deadpool_postgres::{
    Config as PostgresConfig, ManagerConfig, Pool, PoolError, RecyclingMethod, Runtime,
};
use std::time::Duration;
use tokio_postgres::{NoTls, error::SqlState};
use tracing::{error, info, warn};

/// Postgres connection pool with automatic database creation.
///
/// If `auto_create_db` is enabled (default) and the target database doesn't
/// exist, it connects to the `postgres` system database and creates it.
pub struct ConnectionManager {
    pool: Pool,
    db_name: String,
    connection_string: String,
    config: DbkitConfig,
}

impl ConnectionManager {
    /// Connect using a [`DbkitConfig`].
    pub async fn connect(config: DbkitConfig) -> Result<Self, DbkitError> {
        let db_name = Self::extract_db_name(&config.url);
        let connection_string = config.url.clone();

        let mut cfg = PostgresConfig::new();
        cfg.url = Some(config.url.clone());
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: config.pool_size,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(Duration::from_secs(config.connect_timeout_secs)),
                create: Some(Duration::from_secs(config.connect_timeout_secs)),
                recycle: Some(Duration::from_secs(config.connect_timeout_secs)),
            },
            ..Default::default()
        });
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| DbkitError::PoolCreation(e.to_string()))?;

        let final_pool = match pool.get().await {
            Ok(_) => {
                info!("connected to database '{}'", db_name);
                pool
            }
            Err(PoolError::Backend(e)) => {
                if let Some(code) = e.code() {
                    if *code == SqlState::INVALID_CATALOG_NAME {
                        if config.auto_create_db {
                            warn!("database '{}' does not exist, creating...", db_name);
                            Self::create_database_if_missing(&config.url, &db_name).await?;
                            cfg.create_pool(Some(Runtime::Tokio1), NoTls)
                                .map_err(|e| DbkitError::PoolCreation(e.to_string()))?
                        } else {
                            return Err(DbkitError::DatabaseCreation {
                                name: db_name,
                                reason: "database does not exist and auto_create_db is disabled"
                                    .into(),
                            });
                        }
                    } else if *code == SqlState::INVALID_PASSWORD {
                        error!("authentication failed");
                        return Err(DbkitError::AuthFailed);
                    } else if *code == SqlState::TOO_MANY_CONNECTIONS {
                        return Err(DbkitError::TooManyConnections);
                    } else {
                        return Err(DbkitError::Connection(format!(
                            "code {:?}: {}",
                            code, e
                        )));
                    }
                } else {
                    return Err(DbkitError::Connection(e.to_string()));
                }
            }
            Err(e) => {
                return Err(DbkitError::Connection(format!(
                    "could not connect to '{}': {}",
                    db_name, e
                )));
            }
        };

        Ok(Self {
            pool: final_pool,
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

    /// Get the underlying connection pool.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Get a connection from the pool.
    pub async fn get_connection(&self) -> Result<deadpool_postgres::Object, DbkitError> {
        self.pool
            .get()
            .await
            .map_err(|e| DbkitError::Pool(e.to_string()))
    }

    /// Check if the database is reachable.
    pub async fn is_connected(&self) -> bool {
        self.pool.get().await.is_ok()
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

    /// Pool health metrics.
    pub fn pool_status(&self) -> PoolStatus {
        let status = self.pool.status();
        PoolStatus {
            max_size: status.max_size,
            size: status.size,
            available: status.available as usize,
            waiting: status.waiting,
        }
    }

    fn extract_db_name(url: &str) -> String {
        url.rsplit('/')
            .next()
            .unwrap_or("postgres")
            .split('?')
            .next()
            .unwrap_or("postgres")
            .to_string()
    }

    async fn create_database_if_missing(url: &str, db_name: &str) -> Result<(), DbkitError> {
        let base_url = if let Some(pos) = url.rfind('/') {
            format!("{}postgres", &url[..=pos])
        } else {
            return Err(DbkitError::DatabaseCreation {
                name: db_name.to_string(),
                reason: "invalid database URL".into(),
            });
        };

        let (client, connection) = tokio_postgres::connect(&base_url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("connection error during DB creation: {}", e);
            }
        });

        let exists = client
            .query_one("SELECT 1 FROM pg_database WHERE datname = $1", &[&db_name])
            .await
            .is_ok();

        if !exists {
            info!("creating database '{}'...", db_name);
            let create_query = format!("CREATE DATABASE \"{}\"", db_name);
            client
                .batch_execute(&create_query)
                .await
                .map_err(|e| DbkitError::DatabaseCreation {
                    name: db_name.to_string(),
                    reason: e.to_string(),
                })?;
            info!("database '{}' created", db_name);
        }

        Ok(())
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
    pub available: usize,
    /// Number of tasks waiting for a connection.
    pub waiting: usize,
}

impl std::fmt::Display for PoolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pool: {}/{} connections, {} available, {} waiting",
            self.size, self.max_size, self.available, self.waiting
        )
    }
}
