//! Correctness test for [`PgHandler::copy_upsert`] — proves the staging-table
//! upsert actually inserts, updates, and (with no update columns) ignores.
//!
//! Requires a live Postgres. Set `DATABASE_URL`; otherwise the test is skipped.
//!   DATABASE_URL=postgres://localhost/dbkit_test \
//!     cargo test --features postgres-native --test copy_upsert
#![cfg(feature = "postgres-native")]

use dbkit::{ConnectionManager, DbValue, PgHandler};
use sqlx::Row;

const COLS: [&str; 3] = ["id", "name", "val"];

fn row(id: i64, name: &str, val: f64) -> Vec<DbValue> {
    vec![DbValue::Int(id), DbValue::Text(name.into()), DbValue::Float(val)]
}

async fn count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM dbkit_upsert_test")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn fetch(pool: &sqlx::PgPool, id: i64) -> (String, f64) {
    let r = sqlx::query("SELECT name, val FROM dbkit_upsert_test WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    (r.get::<String, _>("name"), r.get::<f64, _>("val"))
}

#[tokio::test]
async fn copy_upsert_inserts_updates_and_ignores() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set — skipping copy_upsert test");
        return;
    };

    let conn = ConnectionManager::new(&url).await.expect("connect");
    let pg = PgHandler::new(conn.pg_native_pool().await.expect("native pool"));
    let pool = pg.pool().clone();

    // Fresh table with a PK so `ON CONFLICT (id)` has an index to target.
    sqlx::query("DROP TABLE IF EXISTS dbkit_upsert_test")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE dbkit_upsert_test (id BIGINT PRIMARY KEY, name TEXT, val DOUBLE PRECISION)",
    )
    .execute(&pool)
    .await
    .unwrap();

    // 1. Initial load — all three rows are inserts.
    let rows = vec![row(1, "a", 1.0), row(2, "b", 2.0), row(3, "c", 3.0)];
    let n = pg
        .copy_upsert("dbkit_upsert_test", &COLS, &["id"], &["name", "val"], &rows)
        .await
        .unwrap();
    assert_eq!(n, 3, "3 rows inserted");
    assert_eq!(count(&pool).await, 3);

    // 2. Upsert — update existing ids 2 & 3, insert new id 4.
    let rows = vec![row(2, "B", 22.0), row(3, "C", 33.0), row(4, "d", 4.0)];
    let n = pg
        .copy_upsert("dbkit_upsert_test", &COLS, &["id"], &["name", "val"], &rows)
        .await
        .unwrap();
    assert_eq!(n, 3, "2 updated + 1 inserted = 3 affected");
    assert_eq!(count(&pool).await, 4, "exactly one new row");

    // id=2 was actually overwritten with the incoming values...
    assert_eq!(fetch(&pool, 2).await, ("B".to_string(), 22.0));
    // ...and id=1, absent from the second batch, is untouched.
    assert_eq!(fetch(&pool, 1).await, ("a".to_string(), 1.0));

    // 3. DO NOTHING (empty update columns) — conflicting id=2 is left as-is,
    //    only the genuinely new id=5 is inserted.
    let rows = vec![row(2, "ignored", 999.0), row(5, "e", 5.0)];
    pg.copy_upsert("dbkit_upsert_test", &COLS, &["id"], &[], &rows)
        .await
        .unwrap();
    assert_eq!(count(&pool).await, 5, "only id=5 added");
    assert_eq!(
        fetch(&pool, 2).await,
        ("B".to_string(), 22.0),
        "DO NOTHING must not overwrite the existing row"
    );

    sqlx::query("DROP TABLE IF EXISTS dbkit_upsert_test")
        .execute(&pool)
        .await
        .unwrap();
}
