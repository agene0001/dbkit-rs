//! Verifies the backend-aware migration runner on SQLite (tracking table DDL,
//! placeholders, dedup-by-hash, and the `applied_at` text cast all work on a
//! non-Postgres backend).
//!
//!   cargo test --features sqlite --test migrations
#![cfg(feature = "sqlite")]

use dbkit::{ConnectionManager, InitializationHandler};

struct TempDb(std::path::PathBuf);
impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn migrations_tracked_on_sqlite() {
    let path = std::env::temp_dir().join(format!("dbkit_migr_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let _guard = TempDb(path.clone());
    let url = format!("sqlite://{}", path.display());

    let conn = ConnectionManager::new(&url).await.expect("connect sqlite");
    let init = InitializationHandler::new(conn.pool().clone(), conn.backend());

    let v1 = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";

    // First application runs the DDL and records it.
    init.run_named_migration("001_users", v1)
        .await
        .expect("apply 001");

    // Re-applying identical content is a no-op (same hash).
    init.run_named_migration("001_users", v1)
        .await
        .expect("re-apply 001 is a no-op");

    // Same name, changed content -> error.
    let changed = init
        .run_named_migration(
            "001_users",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        )
        .await;
    assert!(changed.is_err(), "changed migration content must error");

    // A second migration.
    init.run_named_migration("002_posts", "CREATE TABLE posts (id INTEGER PRIMARY KEY)")
        .await
        .expect("apply 002");

    // Listing reflects both, in order, with non-empty timestamps.
    let applied = init.applied_migrations().await.expect("list applied");
    let names: Vec<&str> = applied.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(names, vec!["001_users", "002_posts"]);
    assert!(
        applied.iter().all(|(_, _, at)| !at.is_empty()),
        "applied_at should render as non-empty text"
    );
}
