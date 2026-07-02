//! No-network micro-benchmarks for the client-side code paths that changed in
//! 0.5.0. Everything here is pure CPU, so it isolates the code delta from
//! Postgres/network noise that plagues the end-to-end bench:
//!
//! 1. sqlx bind cost: owned clone (0.4 style) vs borrowed &str (0.5 style)
//! 2. per-row NULL scan cost (the 0.5 per-row `persistent` guard)
//! 3. COPY escape: char-by-char loop (0.4) vs span-copy (0.5)
//!
//! Run: cargo run --release --bin micro

use sqlx::{AssertSqlSafe, Postgres};
use std::hint::black_box;
use std::time::Instant;

const ROWS: usize = 200_000;
const RUNS: usize = 5;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn bench<F: FnMut()>(label: &str, mut f: F) -> f64 {
    f(); // warmup
    let mut times = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let t = Instant::now();
        f();
        times.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let med = median(times);
    println!("{label:<44} {med:>9.3} ms   ({:.1} ns/row)", med * 1e6 / ROWS as f64);
    med
}

// ---- 3. COPY escape implementations, replicated verbatim ----

/// 0.4.x: char-by-char.
fn escape_old(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
}

/// 0.5.0: span-copy.
fn escape_new(s: &str, out: &mut String) {
    let mut start = 0;
    for (i, b) in s.bytes().enumerate() {
        let esc = match b {
            b'\\' => "\\\\",
            b'\t' => "\\t",
            b'\n' => "\\n",
            b'\r' => "\\r",
            _ => continue,
        };
        out.push_str(&s[start..i]);
        out.push_str(esc);
        start = i + 1;
    }
    out.push_str(&s[start..]);
}

fn main() {
    let names: Vec<String> = (0..ROWS).map(|i| format!("item-{i}")).collect();
    let esc_names: Vec<String> = (0..ROWS).map(|i| format!("it\tem\n{i}\\pad-{i}")).collect();
    let sql = "INSERT INTO bench_items (id, name, val) VALUES ($1, $2, $3)";

    println!("== 1. sqlx bind: owned String clone (0.4) vs borrowed &str (0.5), {ROWS} rows ==");
    let owned = bench("bind owned (name.clone())", || {
        for (i, name) in names.iter().enumerate() {
            let q = sqlx::query::<Postgres>(AssertSqlSafe(sql))
                .bind(i as i64)
                .bind(name.clone())
                .bind(i as f64 * 1.5);
            let _ = black_box(q);
        }
    });
    let borrowed = bench("bind borrowed (name.as_str())", || {
        for (i, name) in names.iter().enumerate() {
            let q = sqlx::query::<Postgres>(AssertSqlSafe(sql))
                .bind(i as i64)
                .bind(name.as_str())
                .bind(i as f64 * 1.5);
            let _ = black_box(q);
        }
    });
    println!("   -> borrowed / owned = {:.3}x\n", borrowed / owned);

    println!("== 2. per-row NULL scan (3-value rows, {ROWS} rows) ==");
    let rows: Vec<[u8; 3]> = (0..ROWS).map(|i| [(i % 15) as u8, 1, 2]).collect();
    bench("has_null scan per row", || {
        let mut n = 0usize;
        for row in &rows {
            // Same shape as `params.iter().any(|v| matches!(v, DbValue::Null))`
            if row.iter().any(|v| *v == 0) {
                n += 1;
            }
        }
        black_box(n);
    });
    println!();

    println!("== 3. COPY escape: char-loop (0.4) vs span-copy (0.5), {ROWS} cells ==");
    for (kind, data) in [("clean", &names), ("escape-heavy", &esc_names)] {
        let old = bench(&format!("escape old, {kind}"), || {
            let mut out = String::with_capacity(64);
            for s in data.iter() {
                out.clear();
                escape_old(s, &mut out);
                black_box(&out);
            }
        });
        let new = bench(&format!("escape new, {kind}"), || {
            let mut out = String::with_capacity(64);
            for s in data.iter() {
                out.clear();
                escape_new(s, &mut out);
                black_box(&out);
            }
        });
        println!("   -> new / old = {:.3}x\n", new / old);
    }
}
