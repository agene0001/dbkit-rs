//! End-to-end test of the transactional → analytical sync pipeline.
//!
//! Writes rows to an on-disk SQLite database via the write path, syncs the
//! table into DuckDB (sqlx rows → Arrow → DuckDB), then reads it back through
//! the analytical engine and checks the values survived the round trip.
//!
//! Only compiled when both `sqlite` and `duckdb` are enabled:
//!   cargo test --features sqlite,duckdb --test sync_roundtrip
#![cfg(all(feature = "sqlite", feature = "duckdb"))]

use dbkit::arrow::array::{Int64Array, StringArray};
use dbkit::{BaseHandler, ConnectionManager, FetchMode, WriteOp};

/// A temp file path that removes itself on drop, so the test cleans up even on
/// panic.
struct TempDb(std::path::PathBuf);
impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn sqlite_to_duckdb_sync_roundtrip() {
    let path = std::env::temp_dir().join(format!("dbkit_sync_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let _guard = TempDb(path.clone());
    let url = format!("sqlite://{}", path.display());

    let conn = ConnectionManager::new(&url).await.expect("connect sqlite");
    let handler = BaseHandler::with_duckdb(conn.pool().clone()).expect("duckdb handler");
    assert!(handler.has_read_engine());

    // --- write side (SQLite, `?` placeholders) ---
    handler
        .execute_write(WriteOp::BatchDDL {
            queries: &["CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)"],
        })
        .await
        .expect("create table");

    for (id, name, qty) in [(1i64, "apple", 5i64), (2, "banana", 3), (3, "cherry", 9)] {
        handler
            .execute_write(WriteOp::Single {
                query: "INSERT INTO items (id, name, qty) VALUES (?, ?, ?)",
                params: vec![id.into(), name.into(), qty.into()],
                mode: FetchMode::None,
            })
            .await
            .expect("insert row");
    }

    // --- sync SQLite -> DuckDB ---
    handler.sync_tables(&["items"]).await.expect("sync items");

    // --- read back via DuckDB, ordered by qty ---
    let batches = handler
        .execute_read("SELECT name, qty FROM items ORDER BY qty", &[])
        .await
        .expect("analytical read");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "all rows should round-trip");

    let batch = &batches[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column is Utf8");
    let qtys = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("qty column is Int64");

    // ordered by qty ascending: banana(3), apple(5), cherry(9)
    assert_eq!(qtys.value(0), 3);
    assert_eq!(names.value(0), "banana");
    assert_eq!(qtys.value(1), 5);
    assert_eq!(names.value(1), "apple");
    assert_eq!(qtys.value(2), 9);
    assert_eq!(names.value(2), "cherry");

    // --- an aggregate, to prove DuckDB is really querying the synced data ---
    // Cast to BIGINT: DuckDB's SUM of a BIGINT widens to HUGEINT (128-bit).
    let agg = handler
        .execute_read("SELECT CAST(SUM(qty) AS BIGINT) AS total FROM items", &[])
        .await
        .expect("aggregate read");
    let total = agg[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum is Int64")
        .value(0);
    assert_eq!(total, 17, "5 + 3 + 9");

    // --- typed read (execute_read_as) restores the old closure-mapped reads ---
    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct Item {
        name: String,
        qty: i64,
    }

    let items: Vec<Item> = handler
        .execute_read_as("SELECT name, qty FROM items ORDER BY qty", &[])
        .await
        .expect("typed read");

    assert_eq!(
        items,
        vec![
            Item { name: "banana".into(), qty: 3 },
            Item { name: "apple".into(), qty: 5 },
            Item { name: "cherry".into(), qty: 9 },
        ]
    );
}
