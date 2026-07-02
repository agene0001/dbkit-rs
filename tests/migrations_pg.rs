//! Migration-runner integration test on Postgres: tracking, dedup-by-hash,
//! changed-content rejection, and the transparent upgrade of pre-0.5
//! `DefaultHasher` hashes to the stable FNV-1a hash.
//!
//! Requires a live Postgres. Set `DATABASE_URL`; otherwise the test is skipped.
//!   DATABASE_URL=postgres://localhost/dbkit_test cargo test --test migrations_pg

use dbkit::{
    BaseHandler, ConnectionManager, DbValue, DbkitError, FetchMode, InitializationHandler, WriteOp,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const MIGRATION_SQL: &str =
    "CREATE TABLE dbkit_migr_pg_test (id BIGINT PRIMARY KEY, note TEXT);
     CREATE INDEX dbkit_migr_pg_test_note ON dbkit_migr_pg_test (note)";

/// The pre-0.5 hash algorithm (`DefaultHasher`), replicated here to plant a
/// legacy-format row and prove the runner upgrades it in place.
fn legacy_hash(sql: &str) -> String {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// The stable hash the runner writes as of 0.5 (FNV-1a 64-bit).
fn fnv_hash(sql: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in sql.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

#[tokio::test]
async fn migrations_tracked_and_legacy_hash_upgraded() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set — skipping migrations_pg test");
        return;
    };

    let conn = ConnectionManager::new(&url).await.expect("connect");
    let handler = BaseHandler::new(conn.pool().clone());
    let init = InitializationHandler::new(conn.pool().clone(), conn.backend());

    // Clean slate (previous runs, including the tracking table itself).
    handler
        .execute_write(WriteOp::BatchDDL {
            queries: &[
                "DROP TABLE IF EXISTS dbkit_migr_pg_test",
                "DROP TABLE IF EXISTS _dbkit_migrations",
            ],
        })
        .await
        .expect("cleanup");

    // 1. First run applies both statements and records the migration.
    init.run_named_migration("m1", MIGRATION_SQL).await.expect("apply m1");
    let applied = init.applied_migrations().await.expect("list");
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].0, "m1");
    assert_eq!(applied[0].1, fnv_hash(MIGRATION_SQL));

    // 2. Re-running with identical content is a no-op — the DDL has no
    //    IF NOT EXISTS, so if the runner wrongly re-applied it, this would fail.
    init.run_named_migration("m1", MIGRATION_SQL).await.expect("skip m1");

    // 3. Same name, changed content → refused.
    let changed = init
        .run_named_migration("m1", "CREATE TABLE dbkit_migr_pg_test (id BIGINT)")
        .await;
    assert!(
        matches!(changed, Err(DbkitError::Migration(_))),
        "changed content must be rejected, got {changed:?}"
    );

    // 4. Legacy-hash upgrade: overwrite the stored hash with the pre-0.5
    //    DefaultHasher value, as a database migrated by dbkit < 0.5 would hold.
    handler
        .execute_write(WriteOp::Single {
            query: "UPDATE _dbkit_migrations SET hash = $1 WHERE name = $2",
            params: vec![
                DbValue::Text(legacy_hash(MIGRATION_SQL)),
                DbValue::Text("m1".into()),
            ],
            mode: FetchMode::None,
        })
        .await
        .expect("plant legacy hash");

    // Same content under the legacy hash → recognized, skipped (no re-apply),
    // and the stored hash is upgraded to the stable FNV-1a value.
    init.run_named_migration("m1", MIGRATION_SQL)
        .await
        .expect("legacy hash must be recognized as unchanged content");
    let applied = init.applied_migrations().await.expect("list after upgrade");
    assert_eq!(applied[0].1, fnv_hash(MIGRATION_SQL), "stored hash upgraded in place");

    // 5. And changed content under a legacy hash is still refused.
    let changed = init
        .run_named_migration("m1", "CREATE TABLE dbkit_migr_pg_test (id BIGINT)")
        .await;
    assert!(matches!(changed, Err(DbkitError::Migration(_))));

    handler
        .execute_write(WriteOp::BatchDDL {
            queries: &["DROP TABLE IF EXISTS dbkit_migr_pg_test"],
        })
        .await
        .expect("cleanup end");
}
