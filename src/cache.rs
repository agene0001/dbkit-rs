use dashmap::DashMap;
use std::sync::Arc;

/// Concurrent key-value cache with named buckets.
///
/// Uses DashMap internally for lock-free concurrent reads. Create named
/// buckets for different entity types (products, listings, etc.).
#[derive(Debug, Clone)]
pub struct Cache {
    buckets: Arc<DashMap<String, Arc<DashMap<String, String>>>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(DashMap::new()),
        }
    }

    /// Create a set of named buckets upfront.
    pub fn with_buckets(names: &[&str]) -> Self {
        let cache = Self::new();
        for name in names {
            cache.buckets.insert(name.to_string(), Arc::new(DashMap::new()));
        }
        cache
    }

    /// Get a value from a named bucket.
    pub fn get(&self, bucket: &str, key: &str) -> Option<String> {
        self.buckets
            .get(bucket)
            .and_then(|b| b.get(key).map(|v| v.clone()))
    }

    /// Set a value in a named bucket (creates the bucket if it doesn't exist).
    pub fn set(&self, bucket: &str, key: String, value: String) {
        self.buckets
            .entry(bucket.to_string())
            .or_insert_with(|| Arc::new(DashMap::new()))
            .insert(key, value);
    }

    /// Remove a value from a named bucket.
    pub fn remove(&self, bucket: &str, key: &str) -> Option<String> {
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

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic() {
        let cache = Cache::with_buckets(&["products", "listings"]);

        cache.set("products", "upc:123".into(), "uuid-1".into());
        assert_eq!(cache.get("products", "upc:123"), Some("uuid-1".into()));
        assert_eq!(cache.get("products", "upc:999"), None);
        assert_eq!(cache.get("listings", "upc:123"), None);
    }

    #[test]
    fn test_cache_auto_create_bucket() {
        let cache = Cache::new();
        cache.set("new_bucket", "key".into(), "value".into());
        assert_eq!(cache.get("new_bucket", "key"), Some("value".into()));
    }

    #[test]
    fn test_cache_clear() {
        let cache = Cache::with_buckets(&["a", "b"]);
        cache.set("a", "k1".into(), "v1".into());
        cache.set("b", "k2".into(), "v2".into());

        cache.clear_bucket("a");
        assert_eq!(cache.get("a", "k1"), None);
        assert_eq!(cache.get("b", "k2"), Some("v2".into()));

        cache.clear_all();
        assert_eq!(cache.get("b", "k2"), None);
    }
}
