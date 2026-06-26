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
