use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbkitError {
    #[error("failed to create connection pool: {0}")]
    PoolCreation(String),

    #[error("connection error: {0}")]
    Connection(String),

    #[error("authentication failed")]
    AuthFailed,

    #[error("too many connections")]
    TooManyConnections,

    #[error("database '{name}' does not exist and auto-create failed: {reason}")]
    DatabaseCreation { name: String, reason: String },

    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("pool error: {0}")]
    Pool(String),

    #[cfg(feature = "duckdb")]
    #[error("DuckDB not initialized — call BaseHandler::with_duckdb()")]
    DuckDbNotInitialized,

    #[cfg(feature = "duckdb")]
    #[error("DuckDB error: {0}")]
    DuckDb(String),

    #[error("expected {expected} row(s), got {actual}")]
    RowCount { expected: String, actual: usize },

    #[cfg(feature = "duckdb")]
    #[error("lock poisoned: {0}")]
    LockPoisoned(String),

    #[cfg(feature = "duckdb")]
    #[error("task join error: {0}")]
    TaskJoin(String),

    #[error("migration failed: {0}")]
    Migration(String),
}
