use dashmap::DashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Concurrent key-value cache with named buckets.
///
/// Uses DashMap internally for lock-free concurrent reads. Create named
/// buckets for different entity types (products, listings, etc.).
///
/// Both key and value types default to `String`, so `Cache` is a drop-in
/// replacement for the previous untyped cache. Use `Cache<i32, MyStruct>`,
/// `Cache<String, Vec<u8>>`, etc. for typed buckets.
#[derive(Debug)]
pub struct Cache<K = String, V = String>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    buckets: Arc<DashMap<String, Arc<DashMap<K, V>>>>,
}

impl<K, V> Clone for Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            buckets: Arc::clone(&self.buckets),
        }
    }
}

impl<K, V> Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(DashMap::new()),
        }
    }

    /// Create a set of named buckets upfront.
    pub fn with_buckets(names: &[&str]) -> Self {
        let cache = Self::new();
        for name in names {
            cache
                .buckets
                .insert(name.to_string(), Arc::new(DashMap::new()));
        }
        cache
    }

    /// Get a value from a named bucket.
    pub fn get(&self, bucket: &str, key: &K) -> Option<V> {
        self.buckets
            .get(bucket)
            .and_then(|b| b.get(key).map(|v| v.clone()))
    }

    /// Set a value in a named bucket (creates the bucket if it doesn't exist).
    pub fn set(&self, bucket: &str, key: K, value: V) {
        self.buckets
            .entry(bucket.to_string())
            .or_insert_with(|| Arc::new(DashMap::new()))
            .insert(key, value);
    }

    /// Remove a value from a named bucket.
    pub fn remove(&self, bucket: &str, key: &K) -> Option<V> {
        self.buckets
            .get(bucket)
            .and_then(|b| b.remove(key).map(|(_, v)| v))
    }

    /// Clear all entries in a specific bucket.
    pub fn clear_bucket(&self, bucket: &str) {
        if let Some(b) = self.buckets.get(bucket) {
            b.clear();
        }
    }

    /// Clear all buckets.
    pub fn clear_all(&self) {
        for entry in self.buckets.iter() {
            entry.value().clear();
        }
    }
}

impl<K, V> Default for Cache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic() {
        let cache: Cache = Cache::with_buckets(&["products", "listings"]);

        cache.set("products", "upc:123".into(), "uuid-1".into());
        assert_eq!(cache.get("products", &"upc:123".into()), Some("uuid-1".into()));
        assert_eq!(cache.get("products", &"upc:999".into()), None);
        assert_eq!(cache.get("listings", &"upc:123".into()), None);
    }

    #[test]
    fn test_cache_auto_create_bucket() {
        let cache: Cache = Cache::new();
        cache.set("new_bucket", "key".into(), "value".into());
        assert_eq!(cache.get("new_bucket", &"key".into()), Some("value".into()));
    }

    #[test]
    fn test_cache_clear() {
        let cache: Cache = Cache::with_buckets(&["a", "b"]);
        cache.set("a", "k1".into(), "v1".into());
        cache.set("b", "k2".into(), "v2".into());

        cache.clear_bucket("a");
        assert_eq!(cache.get("a", &"k1".into()), None);
        assert_eq!(cache.get("b", &"k2".into()), Some("v2".into()));

        cache.clear_all();
        assert_eq!(cache.get("b", &"k2".into()), None);
    }

    #[test]
    fn test_typed_cache_i32_values() {
        let cache: Cache<String, i32> = Cache::with_buckets(&["counts"]);

        cache.set("counts", "page_views".into(), 42);
        cache.set("counts", "sessions".into(), 7);

        assert_eq!(cache.get("counts", &"page_views".into()), Some(42));
        assert_eq!(cache.get("counts", &"sessions".into()), Some(7));
        assert_eq!(cache.get("counts", &"missing".into()), None);

        cache.remove("counts", &"page_views".into());
        assert_eq!(cache.get("counts", &"page_views".into()), None);
    }

    #[test]
    fn test_typed_cache_i32_keys() {
        let cache: Cache<i32, String> = Cache::with_buckets(&["users"]);

        cache.set("users", 1, "alice".into());
        cache.set("users", 2, "bob".into());

        assert_eq!(cache.get("users", &1), Some("alice".into()));
        assert_eq!(cache.get("users", &2), Some("bob".into()));
        assert_eq!(cache.get("users", &3), None);
    }

    #[test]
    fn test_typed_cache_both_generic() {
        let cache: Cache<i32, f64> = Cache::new();

        cache.set("scores", 100, 9.5);
        cache.set("scores", 200, 8.3);

        assert_eq!(cache.get("scores", &100), Some(9.5));
        assert_eq!(cache.get("scores", &200), Some(8.3));
    }

    #[test]
    fn test_typed_cache_vec() {
        let cache: Cache<String, Vec<String>> = Cache::new();

        cache.set(
            "tags",
            "item:1".into(),
            vec!["rust".into(), "database".into()],
        );

        let tags = cache.get("tags", &"item:1".into()).unwrap();
        assert_eq!(tags, vec!["rust".to_string(), "database".to_string()]);
    }

    #[test]
    fn test_typed_cache_struct() {
        #[derive(Debug, Clone, PartialEq)]
        struct Product {
            id: i32,
            name: String,
        }

        let cache: Cache<String, Product> = Cache::new();
        cache.set(
            "products",
            "sku:abc".into(),
            Product {
                id: 1,
                name: "Widget".into(),
            },
        );

        let product = cache.get("products", &"sku:abc".into()).unwrap();
        assert_eq!(product.id, 1);
        assert_eq!(product.name, "Widget");
    }
}
