//! Shared analytical primitives: the Arrow contract and typed deserialization.
//!
//! Both embedded read engines ([`crate::read`]) and remote sources
//! ([`crate::remote`]) speak Arrow `RecordBatch`. To make that one concrete
//! type — usable even in a build with no embedded engine — dbkit depends on the
//! `arrow` crate directly here and re-exports it. Because every analytical
//! dependency (DuckDB, DataFusion) is on the same arrow major, cargo unifies
//! them onto this one crate, so an engine's batches are this exact type.

pub use arrow;
pub use arrow::record_batch::RecordBatch;

/// Deserialize Arrow batches into typed rows via `serde_arrow`.
///
/// Shared by [`BaseHandler::execute_read_as`](crate::BaseHandler::execute_read_as)
/// and remote `query_as`.
#[cfg(feature = "_read-typed")]
pub(crate) fn deserialize_batches<T>(batches: &[RecordBatch]) -> Result<Vec<T>, crate::DbkitError>
where
    T: serde::de::DeserializeOwned,
{
    let mut rows = Vec::new();
    for batch in batches {
        let part: Vec<T> = serde_arrow::from_record_batch(batch)
            .map_err(|e| crate::DbkitError::Conversion(e.to_string()))?;
        rows.extend(part);
    }
    Ok(rows)
}

/// Serialize typed rows into a single Arrow [`RecordBatch`] via `serde_arrow`.
///
/// The Arrow schema is inferred from the row samples. Shared by remote
/// `write_rows`.
#[cfg(feature = "_read-typed")]
pub(crate) fn serialize_rows<T>(rows: &[T]) -> Result<RecordBatch, crate::DbkitError>
where
    T: serde::Serialize,
{
    use serde_arrow::schema::{SchemaLike, TracingOptions};
    let fields = Vec::<arrow::datatypes::FieldRef>::from_samples(rows, TracingOptions::default())
        .map_err(|e| crate::DbkitError::Conversion(e.to_string()))?;
    serde_arrow::to_record_batch(&fields, &rows)
        .map_err(|e| crate::DbkitError::Conversion(e.to_string()))
}
