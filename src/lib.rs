//! Multi-backend database infrastructure.
//!
//! Transactional writes go through [sqlx]'s `Any` driver, so the backend
//! (Postgres, MySQL, or SQLite) is selected from the connection URL scheme.
//! Analytical reads go through a pluggable Arrow-based engine (DuckDB or
//! DataFusion), behind feature flags.
//!
//! - [`ConnectionManager`] — sqlx connection pool with auto-create DB
//! - [`Cache`] — DashMap-based concurrent key-value cache with named buckets
//! - [`BaseHandler`] — `execute_write` (sqlx) / `execute_read` (Arrow) /
//!   `execute_read_as` (typed), plus transactional → analytical sync
//! - [`InitializationHandler`] — backend-aware DDL migration executor
//!
//! [sqlx]: https://docs.rs/sqlx
//!
//! # Example
//! ```no_run
//! use dbkit::ConnectionManager;
//!
//! # async fn example() -> Result<(), dbkit::DbkitError> {
//! // The URL scheme selects the backend: postgres:// , mysql:// , sqlite://
//! let conn = ConnectionManager::new("postgres://localhost/myapp").await?;
//!
//! // `pool()` yields a sqlx `AnyPool`, usable across all supported backends.
//! let _pool = conn.pool();
//! # Ok(())
//! # }
//! ```
//!
//! Note: the write/read handlers and migration runner are being ported onto
//! this pool; see the crate changelog for the current status.

#[cfg(feature = "_arrow")]
mod analytical;
mod base_handler;
mod cache;
pub mod config;
mod connection;
mod error;
mod initialization;
#[cfg(any(feature = "duckdb", feature = "datafusion"))]
mod read;
#[cfg(feature = "clickhouse")]
mod remote;
mod value;

pub use base_handler::{BaseHandler, FetchMode, QueryResult, WriteOp};
pub use cache::Cache;
pub use config::{Backend, ConfigBuilder, DbkitConfig, SslMode};
pub use connection::{ConnectionManager, PoolStatus};
pub use error::DbkitError;
pub use initialization::InitializationHandler;
pub use value::DbValue;

// Arrow contract — available whenever any analytical feature is on (embedded
// engine or remote source). `arrow` is re-exported so callers can inspect
// `RecordBatch`es with a version-matched arrow.
#[cfg(feature = "_arrow")]
pub use analytical::{RecordBatch, arrow};

// Embedded analytical read engines.
#[cfg(any(feature = "duckdb", feature = "datafusion"))]
pub use read::ReadEngine;

// Remote analytical sources (read) and sinks (bulk write).
#[cfg(feature = "clickhouse")]
pub use remote::{
    RemoteSink, RemoteSinkExt, RemoteSource, RemoteSourceExt, clickhouse::ClickHouseSource,
};

// Re-export key sqlx types users will need
pub use sqlx::{AnyPool, any::AnyRow};

// Native Postgres pool (rich types) — see `ConnectionManager::pg_native_pool`.
#[cfg(feature = "postgres-native")]
pub use sqlx::PgPool;
