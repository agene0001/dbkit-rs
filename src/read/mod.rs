//! Engine-agnostic analytical read layer.
//!
//! The contract is Apache Arrow: every [`ReadEngine`] returns its results as
//! `Vec<RecordBatch>`. This lets the analytical engine be swapped (DuckDB or
//! DataFusion) without changing the public surface, and both may be enabled in
//! the same build.
//!
//! [`RecordBatch`] comes from [`crate::analytical`], which depends on `arrow`
//! directly. Because the engines are on the same arrow major, cargo unifies
//! everything onto that one crate, so an engine's batches are exactly this type.

use crate::DbkitError;
use crate::analytical::RecordBatch;
use crate::value::DbValue;
use async_trait::async_trait;

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "datafusion")]
pub mod datafusion;

mod convert;
pub(crate) use convert::rows_to_record_batch;

/// An analytical query engine that returns columnar Arrow data.
#[async_trait]
pub trait ReadEngine: Send + Sync {
    /// Run an analytical query and collect the result as Arrow batches.
    ///
    /// Parameter support is engine-dependent: DuckDB binds `params`; DataFusion
    /// does not yet and returns an error if any are supplied.
    async fn query_arrow(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<Vec<RecordBatch>, DbkitError>;

    /// Materialize a named in-memory table from Arrow batches, replacing any
    /// existing table of the same name. An empty `batches` is a no-op.
    async fn load_table(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DbkitError>;
}

// ---------------------------------------------------------------------------
// Row-mapped read operations (DuckDB)
// ---------------------------------------------------------------------------
// Restores the 0.2.x ergonomic read API: a per-row `map_fn` closure over native
// DuckDB rows returning a typed `QueryResult<T>`, alongside an Arrow variant.
// Driven by [`PgHandler::execute_read`](crate::PgHandler::execute_read). Kept
// DuckDB-specific because the closure receives a `duckdb::Row`.
#[cfg(feature = "duckdb")]
pub use row_op::{ReadOp, ReadResult};

#[cfg(feature = "duckdb")]
mod row_op {
    use super::{DbValue, DbkitError, RecordBatch};
    use crate::base_handler::{FetchMode, QueryResult};

    /// Unified read operation types for the DuckDB row-mapped read path.
    pub enum ReadOp<'a, T, F>
    where
        F: Fn(&::duckdb::Row<'_>) -> Result<T, DbkitError> + Send + 'static,
        T: Send + 'static,
    {
        /// Standard query mapped per row via `map_fn`.
        Standard {
            query: &'a str,
            params: Vec<DbValue>,
            map_fn: F,
            mode: FetchMode,
        },
        /// Arrow columnar query — returns `Vec<RecordBatch>`.
        Arrow {
            query: &'a str,
            params: Vec<DbValue>,
        },
    }

    type NoopMapFn = fn(&::duckdb::Row<'_>) -> Result<(), DbkitError>;

    impl<'a> ReadOp<'a, (), NoopMapFn> {
        /// Convenience constructor for Arrow reads without type annotations.
        pub fn arrow(query: &'a str, params: Vec<DbValue>) -> Self {
            ReadOp::Arrow { query, params }
        }
    }

    /// Result wrapper for DuckDB read queries.
    pub enum ReadResult<T> {
        Standard(QueryResult<T>),
        Arrow(Vec<RecordBatch>),
    }

    impl<T> ReadResult<T> {
        /// Unwrap the standard (row-mapped) result.
        pub fn standard(self) -> Result<QueryResult<T>, DbkitError> {
            match self {
                Self::Standard(qr) => Ok(qr),
                _ => Err(DbkitError::RowCount {
                    expected: "Standard".into(),
                    actual: 0,
                }),
            }
        }

        /// Unwrap the Arrow result.
        pub fn arrow(self) -> Result<Vec<RecordBatch>, DbkitError> {
            match self {
                Self::Arrow(b) => Ok(b),
                _ => Err(DbkitError::RowCount {
                    expected: "Arrow".into(),
                    actual: 0,
                }),
            }
        }
    }
}
