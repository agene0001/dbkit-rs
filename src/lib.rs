//! Reusable Postgres + DuckDB database infrastructure.
//!
//! Provides the foundational components for projects that use Postgres for
//! writes and optionally DuckDB for analytical reads:
//!
//! - [`ConnectionManager`] — Postgres connection pool with auto-create DB
//! - [`Cache`] — DashMap-based concurrent key-value cache with named buckets
//! - [`BaseHandler`] — Unified `execute_write` (Postgres) / `execute_read` (DuckDB)
//! - [`InitializationHandler`] — Batch DDL migration executor
//!
//! # Example
//! ```no_run
//! use dbkit::{ConnectionManager, BaseHandler, InitializationHandler};
//!
//! # async fn example() -> Result<(), dbkit::DbkitError> {
//! let conn = ConnectionManager::new("postgres://localhost/myapp").await?;
//! let pool = std::sync::Arc::new(conn.pool().clone());
//!
//! // Run migrations
//! let init = InitializationHandler::new(pool.clone());
//! let sql = "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT)";
//! init.run_migrations(sql).await?;
//!
//! // Use the handler for queries
//! let handler = BaseHandler::new(pool);
//! # Ok(())
//! # }
//! ```

mod base_handler;
mod cache;
pub mod config;
mod connection;
mod error;
mod initialization;

pub use base_handler::{BaseHandler, FetchMode, QueryResult, WriteOp};
pub use cache::Cache;
pub use config::{ConfigBuilder, DbkitConfig, SslMode};
pub use connection::{ConnectionManager, PoolStatus};
pub use error::DbkitError;
pub use initialization::InitializationHandler;

// DuckDB types only available with the feature
#[cfg(feature = "duckdb")]
pub use base_handler::{DuckParam, ReadOp, ReadResult};

// Re-export key types users will need
pub use deadpool_postgres::Pool;
pub use tokio_postgres::Row as PgRow;
