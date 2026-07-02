//! Integration tests for the multi-backend `Any` pool against Postgres,
//! covering the 0.5 fixes:
//!
//! - `DbValue::Null` binds as a *text* NULL (dbkit < 0.5 bound `int8`, so a
//!   NULL into a varchar/text column failed with "column is of type X but
//!   expression is of type bigint").
//! - NULL-bearing statements re-parse (`persistent(false)`), so a batch whose
//!   NULL/concrete pattern varies row-to-row can't poison the cached prepared
//!   statement's parameter types.
//! - `BatchParams { isolate_rows: true }` wraps rows in SAVEPOINTs: a bad row
//!   is skipped and the rest STILL COMMITS (dbkit < 0.5 lost the whole batch
//!   on Postgres — the transaction aborted and COMMIT silently rolled back —
//!   while returning Ok).
//!
//! Requires a live Postgres. Set `DATABASE_URL`; otherwise the tests skip.

use dbkit::{BaseHandler, ConnectionManager, DbValue, FetchMode, WriteOp};
use sqlx::Row;

async fn setup(table: &str) -> Option<BaseHandler> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set — skipping any_pool_pg test");
        return None;
    };
    let conn = ConnectionManager::new(&url).await.expect("connect");
    let handler = BaseHandler::new(conn.pool().clone());
    handler
        .execute_write(WriteOp::BatchDDL {
            queries: &[
                &format!("DROP TABLE IF EXISTS {table}"),
                &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"),
            ],
        })
        .await
        .expect("create table");
    Some(handler)
}

async fn count(handler: &BaseHandler, table: &str) -> i64 {
    let row = handler
        .execute_write(WriteOp::Single {
            query: &format!("SELECT count(*) FROM {table}"),
            params: vec![],
            mode: FetchMode::One,
        })
        .await
        .expect("count")
        .one()
        .expect("one row");
    row.get::<i64, _>(0)
}

#[tokio::test]
async fn null_binds_into_text_column() {
    let table = "dbkit_any_null_test";
    let Some(handler) = setup(table).await else { return };
    let insert = format!("INSERT INTO {table} (id, name) VALUES ($1, $2)");

    // Concrete text first — pins the cached statement's param types.
    handler
        .execute_write(WriteOp::Single {
            query: &insert,
            params: vec![DbValue::Int(1), DbValue::Text("a".into())],
            mode: FetchMode::None,
        })
        .await
        .expect("insert concrete text");

    // NULL into the text column through the same SQL. Pre-0.5 this failed:
    // int8-typed NULL vs text column (and a poisoned cached statement).
    handler
        .execute_write(WriteOp::Single {
            query: &insert,
            params: vec![DbValue::Int(2), DbValue::Null],
            mode: FetchMode::None,
        })
        .await
        .expect("insert NULL into text column");

    // And concrete text again after the NULL — the cached statement must
    // still be usable.
    handler
        .execute_write(WriteOp::Single {
            query: &insert,
            params: vec![DbValue::Int(3), DbValue::Text("c".into())],
            mode: FetchMode::None,
        })
        .await
        .expect("insert concrete text after NULL");

    let row = handler
        .execute_write(WriteOp::Single {
            query: &format!("SELECT name FROM {table} WHERE id = $1"),
            params: vec![DbValue::Int(2)],
            mode: FetchMode::One,
        })
        .await
        .expect("select")
        .one()
        .expect("one row");
    assert_eq!(row.get::<Option<String>, _>(0), None, "stored value is NULL");
    assert_eq!(count(&handler, table).await, 3);
}

#[tokio::test]
async fn batch_with_mixed_nulls_commits() {
    let table = "dbkit_any_batch_null_test";
    let Some(handler) = setup(table).await else { return };

    // NULL/concrete flips row-to-row for the same column — exercises the
    // per-row persistent(false) guard on the all-or-nothing path.
    let params_list: Vec<Vec<DbValue>> = (0..50i64)
        .map(|i| {
            let name = if i % 3 == 0 {
                DbValue::Null
            } else {
                DbValue::Text(format!("n{i}"))
            };
            vec![DbValue::Int(i), name]
        })
        .collect();

    handler
        .execute_write(WriteOp::BatchParams {
            query: &format!("INSERT INTO {table} (id, name) VALUES ($1, $2)"),
            params_list,
            isolate_rows: false,
        })
        .await
        .expect("mixed NULL batch");
    assert_eq!(count(&handler, table).await, 50);
}

#[tokio::test]
async fn isolated_batch_survives_bad_row() {
    let table = "dbkit_any_isolate_test";
    let Some(handler) = setup(table).await else { return };

    // Row 3 collides with row 2's primary key; with isolate_rows the other
    // three rows must still commit. Pre-0.5 the whole batch was silently lost.
    let params_list: Vec<Vec<DbValue>> = vec![
        vec![DbValue::Int(1), DbValue::Text("one".into())],
        vec![DbValue::Int(2), DbValue::Text("two".into())],
        vec![DbValue::Int(2), DbValue::Text("dup".into())],
        vec![DbValue::Int(3), DbValue::Null],
    ];

    handler
        .execute_write(WriteOp::BatchParams {
            query: &format!("INSERT INTO {table} (id, name) VALUES ($1, $2)"),
            params_list,
            isolate_rows: true,
        })
        .await
        .expect("isolated batch");

    assert_eq!(count(&handler, table).await, 3, "good rows committed, bad row skipped");
    let row = handler
        .execute_write(WriteOp::Single {
            query: &format!("SELECT name FROM {table} WHERE id = $1"),
            params: vec![DbValue::Int(2)],
            mode: FetchMode::One,
        })
        .await
        .expect("select")
        .one()
        .expect("one row");
    assert_eq!(row.get::<String, _>(0), "two", "first writer wins; dup row rolled back");
}
