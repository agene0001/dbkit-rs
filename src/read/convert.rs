//! Conversion from sqlx `Any` rows into Arrow record batches.
//!
//! This is the engine-agnostic glue behind table sync: rows fetched over sqlx
//! (from Postgres / MySQL / SQLite) are turned into Arrow columns, which any
//! [`ReadEngine`](super::ReadEngine) can then ingest.
//!
//! Column types are dispatched on sqlx's `Any` type name, and values are
//! decoded as `Option<T>` so NULLs map onto Arrow nulls — all via sqlx's public
//! API (`AnyValueKind` is not exported, so it is deliberately avoided).

use crate::DbkitError;
use crate::analytical::RecordBatch;
use crate::analytical::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, NullArray, StringBuilder,
};
use crate::analytical::arrow::datatypes::{DataType, Field, Schema};
use sqlx::any::AnyRow;
use sqlx::{Column, Row, TypeInfo};
use std::sync::Arc;

/// Convert a set of sqlx `Any` rows into a single Arrow [`RecordBatch`].
///
/// Returns `None` when there are no rows, since there is no schema to infer.
pub(crate) fn rows_to_record_batch(rows: &[AnyRow]) -> Result<Option<RecordBatch>, DbkitError> {
    let Some(first) = rows.first() else {
        return Ok(None);
    };

    let columns = first.columns();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());

    for (idx, col) in columns.iter().enumerate() {
        let (array, dtype) = build_column(rows, idx, col.type_info().name())?;
        fields.push(Field::new(col.name(), dtype, true));
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| DbkitError::Conversion(e.to_string()))?;
    Ok(Some(batch))
}

/// Build one Arrow array for column `idx`, dispatched on the sqlx `Any` type name.
fn build_column(
    rows: &[AnyRow],
    idx: usize,
    type_name: &str,
) -> Result<(ArrayRef, DataType), DbkitError> {
    /// Decode column `idx` as `Option<$t>` across all rows into `$builder`.
    macro_rules! build {
        ($t:ty, $builder:ty, $dtype:expr) => {{
            let mut b = <$builder>::with_capacity(rows.len());
            for row in rows {
                let v: Option<$t> = row
                    .try_get(idx)
                    .map_err(|e| DbkitError::Conversion(e.to_string()))?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Ok((Arc::new(b.finish()) as ArrayRef, $dtype))
        }};
    }

    match type_name {
        "BOOLEAN" => build!(bool, BooleanBuilder, DataType::Boolean),
        "SMALLINT" => build!(i16, Int16Builder, DataType::Int16),
        "INTEGER" => build!(i32, Int32Builder, DataType::Int32),
        "BIGINT" => build!(i64, Int64Builder, DataType::Int64),
        "REAL" => build!(f32, Float32Builder, DataType::Float32),
        "DOUBLE" => build!(f64, Float64Builder, DataType::Float64),
        "TEXT" => {
            // StringBuilder::append_value takes impl AsRef<str>; String works.
            let mut b = StringBuilder::new();
            for row in rows {
                let v: Option<String> = row
                    .try_get(idx)
                    .map_err(|e| DbkitError::Conversion(e.to_string()))?;
                match v {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Ok((Arc::new(b.finish()) as ArrayRef, DataType::Utf8))
        }
        "BLOB" => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                let v: Option<Vec<u8>> = row
                    .try_get(idx)
                    .map_err(|e| DbkitError::Conversion(e.to_string()))?;
                match v {
                    Some(bytes) => b.append_value(bytes),
                    None => b.append_null(),
                }
            }
            Ok((Arc::new(b.finish()) as ArrayRef, DataType::Binary))
        }
        "NULL" => Ok((Arc::new(NullArray::new(rows.len())) as ArrayRef, DataType::Null)),
        other => Err(DbkitError::Conversion(format!(
            "unsupported column type for Arrow conversion: {other}"
        ))),
    }
}
