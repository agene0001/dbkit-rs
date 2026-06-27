//! Performance comparison: dbkit-rs 0.2 (tokio-postgres) vs 0.3 (sqlx).
//!
//! Requires a live Postgres. Set `DATABASE_URL`, then:
//!   cargo run --release
//!
//! It benchmarks, against the same table:
//!   - single-row INSERT: v2 | v3 `Any` pool | v3 `PgHandler` (native)
//!   - bulk INSERT:        v2 `BatchParams` | v3 `PgHandler::copy_in` (COPY)
//!   - single-row SELECT:  v2 | v3 `PgHandler`
//!
//! The point is to isolate *where* any slowdown lives — the expectation is that
//! v3's native `PgHandler` is ~on par with v2, the `Any` pool is the slow path,
//! and COPY beats both for bulk.

use dbkit_v2::{BaseHandler as Bh2, ConnectionManager as Cm2, FetchMode as Fm2, WriteOp as Wo2};
use dbkit_v3::{
    BaseHandler as Bh3, ConnectionManager as Cm3, DbValue, FetchMode as Fm3, PgHandler,
    WriteOp as Wo3,
};
use std::time::Instant;
use tokio_postgres::types::ToSql;

const N_SINGLE: usize = 2_000;
const N_BULK: usize = 20_000;
const N_SELECT: usize = 2_000;

/// Time a loop of `iters` async ops and print a one-line summary.
macro_rules! bench {
    ($label:expr, $iters:expr, |$i:ident| $body:block) => {{
        let start = Instant::now();
        for $i in 0..$iters {
            $body
        }
        let d = start.elapsed();
        println!(
            "{:<34}{:>8} ops {:>11.3?} {:>12.0} ops/s",
            $label,
            $iters,
            d,
            $iters as f64 / d.as_secs_f64()
        );
    }};
}

fn row_data(i: usize) -> (i64, String, f64) {
    (i as i64, format!("item-{i}"), i as f64 * 1.5)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set — skipping benchmark.");
        eprintln!("e.g. DATABASE_URL=postgres://user:pass@localhost/dbkit_bench cargo run --release");
        return Ok(());
    };

    // --- connect both versions ---
    let conn3 = Cm3::new(&url).await?;
    let bh3 = Bh3::new(conn3.pool().clone());
    let pg = PgHandler::new(conn3.pg_native_pool().await?);

    let conn2 = Cm2::new(&url).await?;
    let h2 = Bh2::new(std::sync::Arc::new(conn2.pool().clone()));

    // --- schema ---
    bh3.execute_write(Wo3::BatchDDL {
        queries: &[
            "DROP TABLE IF EXISTS bench_items",
            "CREATE TABLE bench_items (id BIGINT, name TEXT, val DOUBLE PRECISION)",
        ],
    })
    .await?;

    let truncate = || async {
        bh3.execute_write(Wo3::Single {
            query: "TRUNCATE bench_items",
            params: vec![],
            mode: Fm3::None,
        })
        .await
        .unwrap();
    };

    let insert_sql = "INSERT INTO bench_items (id, name, val) VALUES ($1, $2, $3)";
    println!("\n== single-row INSERT ({N_SINGLE} rows) ==");

    truncate().await;
    bench!("v2 (tokio-postgres)", N_SINGLE, |i| {
        let (id, name, val) = row_data(i);
        let params: [&(dyn ToSql + Sync); 3] = [&id, &name, &val];
        h2.execute_write(Wo2::Single { query: insert_sql, params: &params, mode: Fm2::None })
            .await
            .unwrap();
    });

    truncate().await;
    bench!("v3 Any pool (BaseHandler)", N_SINGLE, |i| {
        let (id, name, val) = row_data(i);
        bh3.execute_write(Wo3::Single {
            query: insert_sql,
            params: vec![id.into(), name.into(), val.into()],
            mode: Fm3::None,
        })
        .await
        .unwrap();
    });

    truncate().await;
    bench!("v3 native (PgHandler)", N_SINGLE, |i| {
        let (id, name, val) = row_data(i);
        pg.execute_write(Wo3::Single {
            query: insert_sql,
            params: vec![id.into(), name.into(), val.into()],
            mode: Fm3::None,
        })
        .await
        .unwrap();
    });

    // --- bulk insert ---
    println!("\n== bulk INSERT ({N_BULK} rows) ==");

    truncate().await;
    {
        let params_list: Vec<Vec<Box<dyn ToSql + Sync + Send>>> = (0..N_BULK)
            .map(|i| {
                let (id, name, val) = row_data(i);
                vec![
                    Box::new(id) as Box<dyn ToSql + Sync + Send>,
                    Box::new(name),
                    Box::new(val),
                ]
            })
            .collect();
        let start = Instant::now();
        h2.execute_write(Wo2::BatchParams { query: insert_sql, params_list })
            .await?;
        let d = start.elapsed();
        println!("{:<34}{:>8} rows {:>11.3?} {:>12.0} rows/s", "v2 BatchParams", N_BULK, d, N_BULK as f64 / d.as_secs_f64());
    }

    truncate().await;
    {
        let rows: Vec<Vec<DbValue>> = (0..N_BULK)
            .map(|i| {
                let (id, name, val) = row_data(i);
                vec![id.into(), name.into(), val.into()]
            })
            .collect();
        let start = Instant::now();
        let n = pg.copy_in("bench_items", &["id", "name", "val"], &rows).await?;
        let d = start.elapsed();
        println!("{:<34}{:>8} rows {:>11.3?} {:>12.0} rows/s", "v3 PgHandler COPY", n, d, n as f64 / d.as_secs_f64());
    }

    // --- single-row SELECT (table already populated) ---
    println!("\n== single-row SELECT ({N_SELECT} lookups) ==");
    let select_sql = "SELECT id, name, val FROM bench_items WHERE id = $1";

    bench!("v2 (tokio-postgres)", N_SELECT, |i| {
        let id = (i % N_BULK) as i64;
        let params: [&(dyn ToSql + Sync); 1] = [&id];
        h2.execute_write(Wo2::Single { query: select_sql, params: &params, mode: Fm2::One })
            .await
            .unwrap();
    });

    bench!("v3 native (PgHandler)", N_SELECT, |i| {
        let id = (i % N_BULK) as i64;
        pg.query(select_sql, vec![id.into()], Fm3::One).await.unwrap();
    });

    bh3.execute_write(Wo3::BatchDDL { queries: &["DROP TABLE IF EXISTS bench_items"] })
        .await?;
    println!("\ndone.");
    Ok(())
}
