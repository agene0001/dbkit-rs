//! Performance comparison: dbkit-rs 0.2 (tokio-postgres) vs 0.3 (sqlx).
//!
//! Requires a live Postgres. Set `DATABASE_URL`, then:
//!   cargo run --release
//!
//! Methodology (so the numbers are trustworthy):
//!   * Every measurement is the MEDIAN of `RUNS` timed runs, after one untimed
//!     warmup run — so connection setup, prepared-statement caching, and cold
//!     OS/Postgres page caches are paid before the clock starts, and a single
//!     unlucky run can't skew the result. `[min · max]` is printed alongside so
//!     you can see the spread.
//!   * v2 and v3 use the SAME `DATABASE_URL` (same host, same TLS mode, same
//!     pool size = 16), so connection cost is identical.
//!   * Comparisons are like-for-like: each protocol is benched against the same
//!     protocol on the other side, so library overhead is never confused with a
//!     protocol difference (e.g. COPY vs row-by-row INSERT).
//!
//! Sections:
//!   1. single-row INSERT, SEQUENTIAL — pure round-trip latency (1 connection):
//!        v2 | v3 `Any` pool | v3 `PgHandler` (native)
//!   2. single-row INSERT, CONCURRENT (16 workers) — pool throughput:
//!        v2 | v3 `PgHandler` (native)
//!   3. bulk INSERT, ROW-BY-ROW (`BatchParams`) — same protocol both sides:
//!        v2 | v3 `PgHandler`   (note: v3 adds a per-row SAVEPOINT; v2 doesn't)
//!   4. bulk INSERT, COPY — same protocol both sides:
//!        v2 (tokio-postgres `copy_in`) | v3 `PgHandler::copy_in`
//!   5. single-row SELECT — indexed primary-key lookup (not a seq scan):
//!        v2 | v3 `PgHandler` (native)

use dbkit_v2::{BaseHandler as Bh2, ConnectionManager as Cm2, FetchMode as Fm2, WriteOp as Wo2};
use dbkit_v3::{
    BaseHandler as Bh3, ConnectionManager as Cm3, DbValue, FetchMode as Fm3, PgHandler,
    WriteOp as Wo3,
};
use futures_util::SinkExt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_postgres::types::ToSql;

const N_SINGLE: usize = 2_000;
const N_CONC: usize = 2_000;
const WORKERS: usize = 16; // matches the default pool size
const N_BULK: usize = 20_000;
/// Larger COPY payload so the text-render path carries measurable weight
/// relative to the network round-trip.
const N_COPY_BIG: usize = 200_000;
const N_SELECT: usize = 2_000;
const RUNS: usize = 3;
/// NULL fractions (percent of `val` set NULL) to sweep in the batch-with-NULLs test.
const NULL_PCTS: &[usize] = &[0, 10, 25, 50, 100];

const INSERT_SQL: &str = "INSERT INTO bench_items (id, name, val) VALUES ($1, $2, $3)";
const SELECT_SQL: &str = "SELECT id, name, val FROM bench_items WHERE id = $1";
const UPSERT_SQL: &str = "INSERT INTO bench_items (id, name, val) VALUES ($1, $2, $3) \
    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, val = EXCLUDED.val";

fn row_data(i: usize) -> (i64, String, f64) {
    (i as i64, format!("item-{i}"), i as f64 * 1.5)
}

/// Print one result line: median + derived rate, with the min/max spread so the
/// reader can judge variance. `durs` is sorted in place.
fn report(label: &str, unit: &str, count: usize, durs: &mut [Duration]) {
    durs.sort();
    let med = durs[durs.len() / 2];
    let min = durs[0];
    let max = durs[durs.len() - 1];
    let rate = count as f64 / med.as_secs_f64();
    println!(
        "{label:<32}{count:>7} {unit:<4} med {med:>10.3?}  {rate:>11.0} {unit}/s   [min {min:.3?} · max {max:.3?}]"
    );
}

/// Time a sequential loop of `iters` async ops, `RUNS` times after one warmup,
/// resetting state with `$reset` before each run (and the warmup). Reports the
/// median. Use `{}` for `$reset` when the body has no side effects (e.g. SELECT).
macro_rules! bench_seq {
    ($label:expr, $unit:expr, $iters:expr, $reset:block, |$i:ident| $body:block) => {{
        // warmup (untimed) — opens the connection, primes the statement cache.
        $reset
        for $i in 0..$iters {
            $body
        }
        let mut durs: Vec<Duration> = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            $reset
            let start = Instant::now();
            for $i in 0..$iters {
                $body
            }
            durs.push(start.elapsed());
        }
        report($label, $unit, $iters, &mut durs);
    }};
}

/// Fire `total` single-row inserts across `workers` concurrent tasks against the
/// v2 (tokio-postgres) handler, returning the wall-clock elapsed. Each worker
/// strides the id space so the rows stay unique (no primary-key collisions) and
/// at most `workers` ops are in flight at once — exercising the pool.
async fn conc_insert_v2(h: Arc<Bh2>, total: usize, workers: usize) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let h = h.clone();
        handles.push(tokio::spawn(async move {
            let mut i = w;
            while i < total {
                let (id, name, val) = row_data(i);
                let params: [&(dyn ToSql + Sync); 3] = [&id, &name, &val];
                h.execute_write(Wo2::Single {
                    query: INSERT_SQL,
                    params: &params,
                    mode: Fm2::None,
                })
                .await
                .unwrap();
                i += workers;
            }
        }));
    }
    for hd in handles {
        hd.await.unwrap();
    }
    start.elapsed()
}

/// Concurrent-insert twin of [`conc_insert_v2`] for the v3 native `PgHandler`.
async fn conc_insert_pg(h: Arc<PgHandler>, total: usize, workers: usize) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let h = h.clone();
        handles.push(tokio::spawn(async move {
            let mut i = w;
            while i < total {
                let (id, name, val) = row_data(i);
                h.execute_write(Wo3::Single {
                    query: INSERT_SQL,
                    params: vec![id.into(), name.into(), val.into()],
                    mode: Fm3::None,
                })
                .await
                .unwrap();
                i += workers;
            }
        }));
    }
    for hd in handles {
        hd.await.unwrap();
    }
    start.elapsed()
}

/// Render `rows` into Postgres `COPY ... FROM STDIN` text format. Mirrors what
/// `PgHandler::copy_in` does internally, so the v2 COPY path pays the same
/// serialization cost inside its timed region as v3 does.
fn copy_payload(rows: &[(i64, String, f64)]) -> String {
    let mut s = String::with_capacity(rows.len() * 24);
    for (id, name, val) in rows {
        s.push_str(&id.to_string());
        s.push('\t');
        for c in name.chars() {
            match c {
                '\\' => s.push_str("\\\\"),
                '\t' => s.push_str("\\t"),
                '\n' => s.push_str("\\n"),
                '\r' => s.push_str("\\r"),
                _ => s.push(c),
            }
        }
        s.push('\t');
        s.push_str(&val.to_string());
        s.push('\n');
    }
    s
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set — skipping benchmark.");
        eprintln!("e.g. DATABASE_URL=postgres://user:pass@localhost/dbkit_bench cargo run --release");
        return Ok(());
    };

    // --- connect both versions (same URL → same host/TLS/pool size) ---
    let conn3 = Cm3::new(&url).await?;
    let bh3 = Bh3::new(conn3.pool().clone());
    let pg = Arc::new(PgHandler::new(conn3.pg_native_pool().await?));

    let conn2 = Cm2::new(&url).await?;
    let h2 = Arc::new(Bh2::new(Arc::new(conn2.pool().clone())));

    println!(
        "methodology: median of {RUNS} timed runs after 1 warmup · pool size 16 · same DATABASE_URL for v2 & v3"
    );

    // --- schema: PRIMARY KEY makes the SELECT an index lookup, not a seq scan ---
    bh3.execute_write(Wo3::BatchDDL {
        queries: &[
            "DROP TABLE IF EXISTS bench_items",
            "CREATE TABLE bench_items (id BIGINT PRIMARY KEY, name TEXT, val DOUBLE PRECISION)",
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

    // ========================================================================
    // 1. single-row INSERT — SEQUENTIAL (round-trip latency, 1 connection)
    // ========================================================================
    println!("\n== single-row INSERT, sequential ({N_SINGLE} rows) — round-trip latency ==");

    bench_seq!("v2 (tokio-postgres)", "ops", N_SINGLE, { truncate().await; }, |i| {
        let (id, name, val) = row_data(i);
        let params: [&(dyn ToSql + Sync); 3] = [&id, &name, &val];
        h2.execute_write(Wo2::Single { query: INSERT_SQL, params: &params, mode: Fm2::None })
            .await
            .unwrap();
    });

    bench_seq!("v3 Any pool (BaseHandler)", "ops", N_SINGLE, { truncate().await; }, |i| {
        let (id, name, val) = row_data(i);
        bh3.execute_write(Wo3::Single {
            query: INSERT_SQL,
            params: vec![id.into(), name.into(), val.into()],
            mode: Fm3::None,
        })
        .await
        .unwrap();
    });

    bench_seq!("v3 native (PgHandler)", "ops", N_SINGLE, { truncate().await; }, |i| {
        let (id, name, val) = row_data(i);
        pg.execute_write(Wo3::Single {
            query: INSERT_SQL,
            params: vec![id.into(), name.into(), val.into()],
            mode: Fm3::None,
        })
        .await
        .unwrap();
    });

    // ========================================================================
    // 2. single-row INSERT — CONCURRENT (pool throughput, 16 in flight)
    // ========================================================================
    println!("\n== single-row INSERT, concurrent ({N_CONC} rows, {WORKERS} workers) — pool throughput ==");

    {
        truncate().await;
        let _ = conc_insert_v2(h2.clone(), N_CONC, WORKERS).await; // warmup
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            durs.push(conc_insert_v2(h2.clone(), N_CONC, WORKERS).await);
        }
        report("v2 (tokio-postgres)", "ops", N_CONC, &mut durs);
    }
    {
        truncate().await;
        let _ = conc_insert_pg(pg.clone(), N_CONC, WORKERS).await; // warmup
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            durs.push(conc_insert_pg(pg.clone(), N_CONC, WORKERS).await);
        }
        report("v3 native (PgHandler)", "ops", N_CONC, &mut durs);
    }

    // ========================================================================
    // 3. bulk INSERT — ROW-BY-ROW (BatchParams): same protocol both sides
    // ========================================================================
    println!("\n== bulk INSERT, row-by-row / BatchParams ({N_BULK} rows) — same protocol ==");
    println!("   (v3 wraps each row in a SAVEPOINT for per-row error isolation; v2 does not)");

    {
        // warmup
        truncate().await;
        let pl: Vec<Vec<Box<dyn ToSql + Sync + Send>>> = (0..N_BULK)
            .map(|i| {
                let (id, name, val) = row_data(i);
                vec![Box::new(id) as Box<dyn ToSql + Sync + Send>, Box::new(name), Box::new(val)]
            })
            .collect();
        h2.execute_write(Wo2::BatchParams { query: INSERT_SQL, params_list: pl }).await?;

        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            let params_list: Vec<Vec<Box<dyn ToSql + Sync + Send>>> = (0..N_BULK)
                .map(|i| {
                    let (id, name, val) = row_data(i);
                    vec![Box::new(id) as Box<dyn ToSql + Sync + Send>, Box::new(name), Box::new(val)]
                })
                .collect();
            let start = Instant::now();
            h2.execute_write(Wo2::BatchParams { query: INSERT_SQL, params_list }).await?;
            durs.push(start.elapsed());
        }
        report("v2 BatchParams", "rows", N_BULK, &mut durs);
    }
    let build_dbv = || -> Vec<Vec<DbValue>> {
        (0..N_BULK)
            .map(|i| {
                let (id, name, val) = row_data(i);
                vec![id.into(), name.into(), val.into()]
            })
            .collect()
    };
    {
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            let params_list = build_dbv();
            let start = Instant::now();
            pg.execute_write(Wo3::BatchParams {
                query: INSERT_SQL,
                params_list,
                isolate_rows: true,
            })
            .await?;
            durs.push(start.elapsed());
        }
        report("v3 BatchParams isolated", "rows", N_BULK, &mut durs);
    }
    {
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            let params_list = build_dbv();
            let start = Instant::now();
            pg.execute_write(Wo3::BatchParams {
                query: INSERT_SQL,
                params_list,
                isolate_rows: false,
            })
            .await?;
            durs.push(start.elapsed());
        }
        report("v3 BatchParams no-savepoint", "rows", N_BULK, &mut durs);
    }

    // ========================================================================
    // 3b. bulk INSERT — row-by-row, swept across NULL fractions.
    //     v2 binds a *typed* NULL (Option::<f64>::None → float8), so its prepared
    //     statement stays reusable at any NULL fraction. v3's DbValue::Null is
    //     *typeless* (OID 0): a batch with ANY null falls to per-row parse
    //     (persistent(false)); 0% NULL reuses one prepared statement. Sweeping the
    //     fraction shows whether the per-row-parse fallback actually costs more.
    // ========================================================================
    println!("\n== bulk INSERT, row-by-row vs NULL fraction ({N_BULK} rows) ==");
    println!("   (v3: 0% NULL reuses 1 prepared stmt; any NULL → per-row parse)");

    // `val` is NULL when (i % 100) < pct, giving ~pct% NULLs.
    let build_v2_null = |pct: usize| -> Vec<Vec<Box<dyn ToSql + Sync + Send>>> {
        (0..N_BULK)
            .map(|i| {
                let (id, name, val) = row_data(i);
                let val_box: Box<dyn ToSql + Sync + Send> = if (i % 100) < pct {
                    Box::new(Option::<f64>::None)
                } else {
                    Box::new(val)
                };
                vec![Box::new(id) as Box<dyn ToSql + Sync + Send>, Box::new(name), val_box]
            })
            .collect()
    };
    let build_v3_null = |pct: usize| -> Vec<Vec<DbValue>> {
        (0..N_BULK)
            .map(|i| {
                let (id, name, val) = row_data(i);
                let v = if (i % 100) < pct { DbValue::Null } else { val.into() };
                vec![id.into(), name.into(), v]
            })
            .collect()
    };

    for &pct in NULL_PCTS {
        let mut d2 = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            let params_list = build_v2_null(pct);
            let start = Instant::now();
            h2.execute_write(Wo2::BatchParams { query: INSERT_SQL, params_list }).await?;
            d2.push(start.elapsed());
        }
        report(&format!("v2 BatchParams @ {pct:>3}% NULL"), "rows", N_BULK, &mut d2);

        let mut d3 = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            let params_list = build_v3_null(pct);
            let start = Instant::now();
            pg.execute_write(Wo3::BatchParams {
                query: INSERT_SQL,
                params_list,
                isolate_rows: false,
            })
            .await?;
            d3.push(start.elapsed());
        }
        report(&format!("v3 no-savepoint @ {pct:>3}% NULL"), "rows", N_BULK, &mut d3);
    }

    // ========================================================================
    // 4. bulk INSERT — COPY: same protocol both sides
    // ========================================================================
    println!("\n== bulk INSERT, COPY ({N_BULK} rows) — same protocol ==");

    let src: Vec<(i64, String, f64)> = (0..N_BULK).map(row_data).collect();
    let src_dbv: Vec<Vec<DbValue>> = src
        .iter()
        .map(|(id, name, val)| vec![(*id).into(), name.clone().into(), (*val).into()])
        .collect();

    {
        // v2: drive tokio-postgres COPY directly (no COPY in the v2 dbkit API).
        // Acquire + render + send + finish are all timed, matching v3's copy_in.
        let run_v2_copy = || async {
            let client = conn2.pool().get().await.unwrap();
            let payload = copy_payload(&src);
            let sink = client
                .copy_in("COPY bench_items (id, name, val) FROM STDIN")
                .await
                .unwrap();
            futures_util::pin_mut!(sink);
            sink.send(bytes::Bytes::from(payload)).await.unwrap();
            sink.finish().await.unwrap()
        };

        truncate().await;
        let _ = run_v2_copy().await; // warmup
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            let start = Instant::now();
            let _ = run_v2_copy().await;
            durs.push(start.elapsed());
        }
        report("v2 COPY (tokio-postgres)", "rows", N_BULK, &mut durs);
    }
    {
        truncate().await;
        let _ = pg.copy_in("bench_items", &["id", "name", "val"], &src_dbv).await?; // warmup
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            let start = Instant::now();
            let _ = pg.copy_in("bench_items", &["id", "name", "val"], &src_dbv).await?;
            durs.push(start.elapsed());
        }
        report("v3 COPY (PgHandler)", "rows", N_BULK, &mut durs);
    }

    // ========================================================================
    // 4c. bulk INSERT, COPY — large payload, clean vs escape-heavy text.
    //     At 200k rows the render path (escaping + buffer writes) is a visible
    //     fraction of the total; "clean" text has no COPY escape chars (the
    //     common case), "escapes" forces the per-char escape path on every cell.
    // ========================================================================
    println!("\n== bulk INSERT, COPY large ({N_COPY_BIG} rows) — render-path weight ==");

    let big_clean: Vec<Vec<DbValue>> = (0..N_COPY_BIG)
        .map(|i| {
            let (id, name, val) = row_data(i);
            vec![id.into(), name.into(), val.into()]
        })
        .collect();
    let big_esc: Vec<Vec<DbValue>> = (0..N_COPY_BIG)
        .map(|i| {
            let (id, _, val) = row_data(i);
            vec![id.into(), format!("it\tem\n{i}\\pad-{i}").into(), val.into()]
        })
        .collect();

    for (label, rows) in [
        ("v3 COPY 200k clean text", &big_clean),
        ("v3 COPY 200k escape-heavy", &big_esc),
    ] {
        truncate().await;
        let _ = pg.copy_in("bench_items", &["id", "name", "val"], rows).await?; // warmup
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            truncate().await;
            let start = Instant::now();
            let n = pg.copy_in("bench_items", &["id", "name", "val"], rows).await?;
            assert_eq!(n as usize, N_COPY_BIG);
            durs.push(start.elapsed());
        }
        report(label, "rows", N_COPY_BIG, &mut durs);
    }

    // ========================================================================
    // 4b. bulk UPSERT — all rows conflict, ON CONFLICT DO UPDATE
    //     Row-by-row ON CONFLICT (v2 / v3) vs copy_upsert (COPY → staging →
    //     one set-based INSERT…SELECT…ON CONFLICT). The table is pre-loaded each
    //     run so every upserted row hits a conflict (the update path).
    // ========================================================================
    println!("\n== bulk UPSERT, all {N_BULK} rows conflict — ON CONFLICT DO UPDATE ==");

    let build_v2 = || -> Vec<Vec<Box<dyn ToSql + Sync + Send>>> {
        src.iter()
            .map(|(id, name, val)| {
                vec![
                    Box::new(*id) as Box<dyn ToSql + Sync + Send>,
                    Box::new(name.clone()),
                    Box::new(*val),
                ]
            })
            .collect()
    };
    let prepopulate = || async {
        truncate().await;
        pg.copy_in("bench_items", &["id", "name", "val"], &src_dbv).await.unwrap();
    };

    {
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            prepopulate().await;
            let params_list = build_v2();
            let start = Instant::now();
            h2.execute_write(Wo2::BatchParams { query: UPSERT_SQL, params_list }).await?;
            durs.push(start.elapsed());
        }
        report("v2 BatchParams ON CONFLICT", "rows", N_BULK, &mut durs);
    }
    {
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            prepopulate().await;
            let params_list = build_dbv();
            let start = Instant::now();
            pg.execute_write(Wo3::BatchParams {
                query: UPSERT_SQL,
                params_list,
                isolate_rows: false,
            })
            .await?;
            durs.push(start.elapsed());
        }
        report("v3 BatchParams ON CONFLICT", "rows", N_BULK, &mut durs);
    }
    {
        let mut durs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            prepopulate().await;
            let start = Instant::now();
            pg.copy_upsert(
                "bench_items",
                &["id", "name", "val"],
                &["id"],
                &["name", "val"],
                &src_dbv,
            )
            .await?;
            durs.push(start.elapsed());
        }
        report("v3 copy_upsert (staging)", "rows", N_BULK, &mut durs);
    }

    // ========================================================================
    // 5. single-row SELECT — indexed primary-key lookup
    // ========================================================================
    println!("\n== single-row SELECT, indexed PK lookup ({N_SELECT} lookups) ==");

    // Populate once so both versions read the same warm, fully-loaded table.
    truncate().await;
    pg.copy_in("bench_items", &["id", "name", "val"], &src_dbv).await?;

    bench_seq!("v2 (tokio-postgres)", "ops", N_SELECT, {}, |i| {
        let id = (i % N_BULK) as i64;
        let params: [&(dyn ToSql + Sync); 1] = [&id];
        h2.execute_write(Wo2::Single { query: SELECT_SQL, params: &params, mode: Fm2::One })
            .await
            .unwrap();
    });

    bench_seq!("v3 native (PgHandler)", "ops", N_SELECT, {}, |i| {
        let id = (i % N_BULK) as i64;
        pg.query(SELECT_SQL, vec![id.into()], Fm3::One).await.unwrap();
    });

    bh3.execute_write(Wo3::BatchDDL { queries: &["DROP TABLE IF EXISTS bench_items"] })
        .await?;
    println!("\ndone.");
    Ok(())
}
