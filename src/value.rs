//! Backend-neutral parameter values.
//!
//! [`DbValue`] is the single owned value type used for binding parameters on
//! both the write side (sqlx) and the read side (analytical engines). It
//! replaces backend-specific parameter types (such as `tokio_postgres::ToSql`
//! and the old DuckDB-specific param enum) so the same value can flow to any
//! supported database without per-backend conversion at the call site.

/// A backend-neutral parameter value.
///
/// Construct variants directly, or use the `From` conversions for ergonomics —
/// including `Option<T>`, which maps `None` to [`DbValue::Null`]:
///
/// ```
/// use dbkit::DbValue;
///
/// let a: DbValue = 42i64.into();          // Int(42)
/// let b: DbValue = "hello".into();        // Text("hello")
/// let c: DbValue = Some(3.5f64).into();   // Float(3.5)
/// let d: DbValue = None::<i64>.into();     // Null
/// assert_eq!(d, DbValue::Null);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum DbValue {
    /// SQL `NULL`.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer. Smaller integer types widen into this.
    Int(i64),
    /// 64-bit floating point.
    Float(f64),
    /// UTF-8 text.
    Text(String),
    /// Raw bytes (`BYTEA` / `BLOB`).
    Bytes(Vec<u8>),

    // ---- Rich Postgres types (native pool only) ----
    // These carry types the multi-backend `Any` pool can't represent. They bind
    // natively through [`PgHandler`](crate::PgHandler) (sqlx Postgres). On the
    // `Any` write path and the DuckDB read path they fall back to a text/ISO
    // rendering, which is enough for filters and Postgres' assignment casts.
    /// SQL `DATE`.
    #[cfg(feature = "postgres-native")]
    Date(sqlx::types::chrono::NaiveDate),
    /// SQL `TIMESTAMP` (no time zone).
    #[cfg(feature = "postgres-native")]
    DateTime(sqlx::types::chrono::NaiveDateTime),
    /// SQL `TIMESTAMPTZ` (UTC).
    #[cfg(feature = "postgres-native")]
    TimestampTz(sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>),
    /// SQL `JSON` / `JSONB`.
    #[cfg(feature = "postgres-native")]
    Json(sqlx::types::JsonValue),
    /// SQL `UUID`.
    #[cfg(feature = "postgres-native")]
    Uuid(sqlx::types::Uuid),
    /// SQL `TIME` (no time zone).
    #[cfg(feature = "postgres-native")]
    Time(sqlx::types::chrono::NaiveTime),
    /// SQL `TEXT[]`.
    #[cfg(feature = "postgres-native")]
    TextArray(Vec<String>),
    /// SQL `FLOAT8[]` (double precision array).
    #[cfg(feature = "postgres-native")]
    FloatArray(Vec<f64>),
    /// SQL `FLOAT8[]` with nullable elements.
    #[cfg(feature = "postgres-native")]
    OptFloatArray(Vec<Option<f64>>),
}

#[cfg(feature = "postgres-native")]
impl From<sqlx::types::chrono::NaiveDate> for DbValue {
    fn from(v: sqlx::types::chrono::NaiveDate) -> Self {
        DbValue::Date(v)
    }
}

#[cfg(feature = "postgres-native")]
impl From<sqlx::types::chrono::NaiveDateTime> for DbValue {
    fn from(v: sqlx::types::chrono::NaiveDateTime) -> Self {
        DbValue::DateTime(v)
    }
}

#[cfg(feature = "postgres-native")]
impl From<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>> for DbValue {
    fn from(v: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>) -> Self {
        DbValue::TimestampTz(v)
    }
}

#[cfg(feature = "postgres-native")]
impl From<sqlx::types::JsonValue> for DbValue {
    fn from(v: sqlx::types::JsonValue) -> Self {
        DbValue::Json(v)
    }
}

#[cfg(feature = "postgres-native")]
impl From<sqlx::types::Uuid> for DbValue {
    fn from(v: sqlx::types::Uuid) -> Self {
        DbValue::Uuid(v)
    }
}

#[cfg(feature = "postgres-native")]
impl From<sqlx::types::chrono::NaiveTime> for DbValue {
    fn from(v: sqlx::types::chrono::NaiveTime) -> Self {
        DbValue::Time(v)
    }
}

#[cfg(feature = "postgres-native")]
impl From<Vec<String>> for DbValue {
    fn from(v: Vec<String>) -> Self {
        DbValue::TextArray(v)
    }
}

#[cfg(feature = "postgres-native")]
impl From<Vec<f64>> for DbValue {
    fn from(v: Vec<f64>) -> Self {
        DbValue::FloatArray(v)
    }
}

#[cfg(feature = "postgres-native")]
impl From<Vec<Option<f64>>> for DbValue {
    fn from(v: Vec<Option<f64>>) -> Self {
        DbValue::OptFloatArray(v)
    }
}

impl From<bool> for DbValue {
    fn from(v: bool) -> Self {
        DbValue::Bool(v)
    }
}

macro_rules! impl_from_int {
    ($($t:ty),*) => {
        $(
            impl From<$t> for DbValue {
                fn from(v: $t) -> Self {
                    DbValue::Int(v as i64)
                }
            }
        )*
    };
}
impl_from_int!(i8, i16, i32, i64, u8, u16, u32);

impl From<f32> for DbValue {
    fn from(v: f32) -> Self {
        DbValue::Float(v as f64)
    }
}

impl From<f64> for DbValue {
    fn from(v: f64) -> Self {
        DbValue::Float(v)
    }
}

impl From<&str> for DbValue {
    fn from(v: &str) -> Self {
        DbValue::Text(v.to_string())
    }
}

impl From<String> for DbValue {
    fn from(v: String) -> Self {
        DbValue::Text(v)
    }
}

impl From<Vec<u8>> for DbValue {
    fn from(v: Vec<u8>) -> Self {
        DbValue::Bytes(v)
    }
}

impl From<&[u8]> for DbValue {
    fn from(v: &[u8]) -> Self {
        DbValue::Bytes(v.to_vec())
    }
}

/// `None` becomes [`DbValue::Null`]; `Some(x)` delegates to `x`'s conversion.
impl<T: Into<DbValue>> From<Option<T>> for DbValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => DbValue::Null,
        }
    }
}

/// Render a text array as a Postgres array literal (`{"a","b"}`), quoting and
/// escaping each element. Used by the text-fallback binding paths (the `Any`
/// write pool and the DuckDB read engine, neither of which carries native
/// arrays); the native [`PgHandler`](crate::PgHandler) binds arrays directly.
#[cfg(feature = "postgres-native")]
pub(crate) fn pg_text_array_literal(items: &[String]) -> String {
    let inner = items
        .iter()
        .map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{inner}}}")
}

/// Render a (possibly nullable) float array as a Postgres array literal
/// (`{1,2,NULL}`). See [`pg_text_array_literal`] for the rationale.
#[cfg(feature = "postgres-native")]
pub(crate) fn pg_float_array_literal<I: IntoIterator<Item = Option<f64>>>(items: I) -> String {
    let inner = items
        .into_iter()
        .map(|o| o.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{inner}}}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_widen_to_int() {
        assert_eq!(DbValue::from(7i32), DbValue::Int(7));
        assert_eq!(DbValue::from(7u8), DbValue::Int(7));
        assert_eq!(DbValue::from(-3i64), DbValue::Int(-3));
    }

    #[test]
    fn text_from_str_and_string() {
        assert_eq!(DbValue::from("hi"), DbValue::Text("hi".into()));
        assert_eq!(DbValue::from(String::from("hi")), DbValue::Text("hi".into()));
    }

    #[test]
    fn option_maps_none_to_null() {
        let some: DbValue = Some(5i32).into();
        let none: DbValue = None::<i32>.into();
        assert_eq!(some, DbValue::Int(5));
        assert_eq!(none, DbValue::Null);
    }

    #[test]
    fn bytes_conversions() {
        let v: DbValue = vec![1u8, 2, 3].into();
        assert_eq!(v, DbValue::Bytes(vec![1, 2, 3]));
        let s: DbValue = [4u8, 5].as_slice().into();
        assert_eq!(s, DbValue::Bytes(vec![4, 5]));
    }
}
