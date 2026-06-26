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
