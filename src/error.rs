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

    #[error("unsupported database backend: {0}")]
    UnsupportedBackend(String),

    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("pool error: {0}")]
    Pool(String),

    #[cfg(feature = "duckdb")]
    #[error("DuckDB not initialized — call BaseHandler::with_duckdb()")]
    DuckDbNotInitialized,

    #[cfg(feature = "duckdb")]
    #[error("DuckDB error: {0}")]
    DuckDb(String),

    #[cfg(feature = "datafusion")]
    #[error("DataFusion error: {0}")]
    DataFusion(String),

    #[error("no analytical read engine configured")]
    NoReadEngine,

    #[error("remote source error: {0}")]
    Remote(String),

    #[error("type conversion error: {0}")]
    Conversion(String),

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

    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}
