//! Remote analytical sources.
//!
//! A [`RemoteSource`] is an external analytical database (e.g. ClickHouse) that
//! you **query over the network** — the data already lives there, so unlike an
//! embedded [`ReadEngine`](crate::ReadEngine) there is no `load_table` or sync.
//! The contract is the same Arrow `RecordBatch`, so typed deserialization works
//! identically via [`RemoteSourceExt::query_as`].

use crate::DbkitError;
use crate::analytical::RecordBatch;
use crate::value::DbValue;
use async_trait::async_trait;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;

/// An external analytical database queried over the network, returning Arrow.
///
/// Object-safe (only `query_arrow`), so `Box<dyn RemoteSource>` works. Typed
/// reads are provided by the [`RemoteSourceExt`] extension trait.
#[async_trait]
pub trait RemoteSource: Send + Sync {
    /// Run an analytical query against the remote store, returning Arrow batches.
    ///
    /// Parameter support is source-dependent; sources that don't support bind
    /// parameters return an error if any are supplied.
    async fn query_arrow(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<Vec<RecordBatch>, DbkitError>;
}

/// Typed querying for any [`RemoteSource`].
///
/// Kept separate from `RemoteSource` so the base trait stays object-safe — this
/// generic method is provided by a blanket impl and is callable on concrete
/// sources and on `&dyn RemoteSource` alike.
#[cfg(feature = "_read-typed")]
#[async_trait]
pub trait RemoteSourceExt: RemoteSource {
    /// Run a query and deserialize each row into `T` (via `serde_arrow`).
    async fn query_as<T>(&self, sql: &str, params: &[DbValue]) -> Result<Vec<T>, DbkitError>
    where
        T: serde::de::DeserializeOwned,
    {
        let batches = self.query_arrow(sql, params).await?;
        crate::analytical::deserialize_batches(&batches)
    }
}

#[cfg(feature = "_read-typed")]
impl<S: RemoteSource + ?Sized> RemoteSourceExt for S {}

/// A remote analytical store you can bulk-load Arrow data into.
///
/// The write-side mirror of [`RemoteSource`]: warehouse loads are bulk,
/// columnar, and append-oriented, so the input is Arrow batches (not row-by-row
/// transactional writes). Object-safe; typed writes come from [`RemoteSinkExt`].
#[async_trait]
pub trait RemoteSink: Send + Sync {
    /// Append Arrow batches to `table` in the remote store. The table is
    /// expected to already exist with a compatible schema. Empty input is a
    /// no-op.
    async fn write_arrow(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DbkitError>;
}

/// Typed writing for any [`RemoteSink`].
#[cfg(feature = "_read-typed")]
#[async_trait]
pub trait RemoteSinkExt: RemoteSink {
    /// Serialize `rows` to Arrow (via `serde_arrow`) and append them to `table`.
    async fn write_rows<T>(&self, table: &str, rows: &[T]) -> Result<(), DbkitError>
    where
        T: serde::Serialize + Sync,
    {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = crate::analytical::serialize_rows(rows)?;
        self.write_arrow(table, vec![batch]).await
    }
}

#[cfg(feature = "_read-typed")]
impl<S: RemoteSink + ?Sized> RemoteSinkExt for S {}
