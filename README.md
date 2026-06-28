# dbkit-rs

Multi-backend database infrastructure for Rust applications.

Transactional writes go through [sqlx]'s `Any` driver — the backend (Postgres,
MySQL, or SQLite) is chosen by the connection URL scheme. Analytical reads go
through a pluggable, Arrow-based engine (DuckDB or DataFusion) behind feature
flags.

> **Upgrading from 0.2?** See the [migration guide](MIGRATING.md).

## Features

- **Multi-backend writes** — `ConnectionManager` + `BaseHandler` over sqlx; `postgres://`, `mysql://`, or `sqlite://` selects the backend
- **Connection pooling** — auto-create database, configurable pool size and timeouts
- **Configurable** — `DbkitConfig` builder with URL construction, SSL modes, and pool tuning
- **Unified writes** — `execute_write` with `WriteOp` (single / batch DDL / batch params) and neutral `DbValue` parameters
- **Bulk Postgres writes** — `PgHandler::copy_in` (COPY) and `copy_upsert` (COPY → staging → set-based `ON CONFLICT`) for high-volume inserts/upserts
- **Analytical reads** — `execute_read` returns `Vec<RecordBatch>` (Arrow); `execute_read_as::<T>` deserializes rows into typed structs via `serde_arrow`
- **Pluggable read engines** — DuckDB or DataFusion (both may be enabled together; they share an Arrow version)
- **Transactional → analytical sync** — `sync_tables()` / `sync_query()` copy data into the read engine (any backend × any engine); DuckDB can also attach Postgres live
- **Backend-aware migrations** — `InitializationHandler` with named migrations tracked by content hash, portable across Postgres/MySQL/SQLite
- **Concurrent cache** — generic DashMap-based key-value cache with named buckets
- **Unicode normalization** — `BaseHandler::normalize_name()` for consistent name matching via NFD decomposition

## Usage

```rust
use dbkit::{BaseHandler, ConnectionManager, DbkitConfig, FetchMode, WriteOp};

// Connect — the URL scheme picks the backend (postgres:// / mysql:// / sqlite://)
let conn = ConnectionManager::new("postgres://localhost/myapp").await?;

// Or use the config builder
let config = DbkitConfig::builder()
    .host("localhost")
    .database("myapp")
    .user("admin")
    .password("secret")
    .pool_size(32)
    .build();
let conn = ConnectionManager::connect(config).await?;

// `pool()` yields a sqlx `AnyPool` (cheaply cloneable)
let handler = BaseHandler::new(conn.pool().clone());

// Write. Placeholders are backend-native: `$1` for Postgres, `?` for MySQL/SQLite.
handler.execute_write(WriteOp::Single {
    query: "INSERT INTO users (name) VALUES ($1)",
    params: vec!["Alice".into()],
    mode: FetchMode::None,
}).await?;
```

### Migrations

`InitializationHandler` tracks named migrations by content hash in a
`_dbkit_migrations` table whose DDL adapts to the backend.

```rust
use dbkit::InitializationHandler;

let init = InitializationHandler::new(conn.pool().clone(), conn.backend());
init.run_named_migration("001_users", "
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    )
").await?;
```

The tracking table is portable, but the *migration SQL you supply* is
backend-native — write it for the database you connected to.

### Pool health

```rust
let status = conn.pool_status();
println!("connections: {}/{}, idle: {}", status.size, status.max_size, status.idle);
```

## Analytical reads (optional)

Enable a read engine. DuckDB and DataFusion share the Arrow `RecordBatch`
contract, so either can be used the same way:

```toml
dbkit = { version = "0.4", features = ["duckdb"] }       # or "datafusion"
```

Attach a read engine to the handler:

```rust
// DuckDB (in-memory, bundled)
let handler = BaseHandler::with_duckdb(conn.pool().clone())?;

// or DataFusion (pure-Rust, no FFI)
let handler = BaseHandler::with_datafusion(conn.pool().clone())?;
```

### Sync, then read

Copy transactional tables into the analytical engine, then query them:

```rust
use dbkit::DbValue;

// Whole tables
handler.sync_tables(&["users", "orders"]).await?;

// A filtered / arbitrary query, loaded as a named table
handler.sync_query(
    "recent_orders",
    "SELECT * FROM orders WHERE created_at > $1",
    &[DbValue::Text("2024-01-01".into())],
).await?;

// Arrow read
let batches = handler.execute_read("SELECT * FROM recent_orders", &[]).await?;

// ...or deserialize straight into typed rows
#[derive(serde::Deserialize)]
struct Order { id: i64, total: f64 }
let orders: Vec<Order> =
    handler.execute_read_as("SELECT id, total FROM recent_orders", &[]).await?;
```

### Live Postgres attach (DuckDB)

DuckDB can query Postgres tables directly — no copy — via the `pg` catalog:

```rust
let handler = BaseHandler::with_duckdb_attached_postgres(
    conn.pool().clone(),
    "postgresql://localhost/myapp",
)?;
let batches = handler.execute_read("SELECT * FROM pg.public.users", &[]).await?;
```

### Rich Postgres types (uuid / timestamps / json)

The multi-backend `Any` pool only represents basic scalars (bool / int / float /
text / bytes) — the price of Postgres/MySQL/SQLite interchangeability. For
queries involving native Postgres types (`uuid`, `timestamptz`, `jsonb`, arrays,
decimal), enable `postgres-native` and drop to a real `PgPool` with full sqlx
type support:

```toml
dbkit = { version = "0.4", features = ["postgres-native"] }
```

```rust
let pg = conn.pg_native_pool().await?;   // native PgPool, rich types
let row = sqlx::query("SELECT id, created_at FROM users WHERE id = $1")
    .bind(some_uuid)
    .fetch_one(&pg)
    .await?;
let id: sqlx::types::Uuid = row.get("id");
let ts: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc> = row.get("created_at");
```

Use the `Any` pool (`handler.execute_write`) for portable work, and the native
pool only where you need Postgres-specific types. (Other type features such as
`rust_decimal` can be enabled by adding `sqlx` to your own `Cargo.toml` with the
relevant feature — cargo unifies it with dbkit's.)

### Bulk writes (Postgres)

On a native `PgPool` (`postgres-native`), `PgHandler` offers three write paths.
Pick by what the operation needs:

| Use… | When |
|------|------|
| `copy_in` | Plain bulk **insert** into one table. Fastest (~30–50× row-by-row). All-or-nothing; no `ON CONFLICT`. |
| `copy_upsert` | Bulk **upsert**: `COPY` → temp staging table → one set-based `INSERT…SELECT…ON CONFLICT` (~10× faster than row-by-row `ON CONFLICT`). |
| `WriteOp::BatchParams` | Anything COPY can't do — see the four cases below. |

```rust
use dbkit::{DbValue, PgHandler, WriteOp};

let pg = PgHandler::new(conn.pg_native_pool().await?);
let rows = vec![
    vec![DbValue::Int(1), DbValue::Text("a".into())],
    vec![DbValue::Int(2), DbValue::Text("b".into())],
];

// Fastest plain bulk insert.
pg.copy_in("items", &["id", "name"], &rows).await?;

// Bulk upsert: overwrite `name` on id conflict. An empty update list ⇒ DO NOTHING.
pg.copy_upsert("items", &["id", "name"], &["id"], &["name"], &rows).await?;

// Row-by-row batch. `isolate_rows: false` = fast all-or-nothing;
// `true` = wrap each row in a SAVEPOINT and skip bad rows.
pg.execute_write(WriteOp::BatchParams {
    query: "INSERT INTO items (id, name) VALUES ($1, $2) \
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
    params_list: rows,
    isolate_rows: false,
}).await?;
```

`copy_upsert` is the default for bulk inserts/upserts into a Postgres table, but
**`BatchParams` stays essential for four things COPY/staging genuinely can't do:**

1. **Non-INSERT statements** — `BatchParams` runs *any* query per param set
   (`UPDATE … WHERE`, `DELETE`, function calls, multi-CTE). Staging only does
   insert-shaped loads.
2. **Per-row error isolation** — `ON CONFLICT` only resolves unique/exclusion
   conflicts; a CHECK/FK/NOT-NULL violation or bad value aborts the whole staged
   `INSERT…SELECT`. `BatchParams { isolate_rows: true }` skips *any* bad row and
   commits the rest.
3. **Non-Postgres backends** — `copy_*` are Postgres-only; `BatchParams` runs on
   the `Any` pool (MySQL/SQLite).
4. **Small batches** — the temp-table + COPY + insert-select setup has fixed
   overhead; for tens of rows, plain `BatchParams` (or a single multi-row INSERT)
   wins.

`copy_in` / `copy_upsert` are all-or-nothing and Postgres-only. Within one
`copy_upsert` call the conflict key must be unique across `rows` (duplicate keys
make `ON CONFLICT DO UPDATE` error — de-duplicate first).

### Name normalization

```rust
let normalized = BaseHandler::normalize_name("José García");
assert_eq!(normalized, "jose\u{301} garci\u{301}a");
```

### Cache

Both key and value types are generic, defaulting to `String`:

```rust
use dbkit::Cache;

let cache: Cache = Cache::with_buckets(&["products", "prices"]);
cache.set("products", "abc123".into(), "Widget".into());
let val = cache.get("products", &"abc123".into());

// Typed keys and values
let counts: Cache<String, i32> = Cache::with_buckets(&["metrics"]);
counts.set("metrics", "page_views".into(), 42);
assert_eq!(counts.get("metrics", &"page_views".into()), Some(42));
```

## Feature flags

| Feature      | Default | Description |
|--------------|---------|-------------|
| `postgres`   | **on**  | Postgres backend (sqlx) |
| `postgres-native` | off | Native `PgPool` with full Postgres types (uuid/chrono/json) — see below |
| `mysql`      | off     | MySQL backend (sqlx) |
| `sqlite`     | off     | SQLite backend (sqlx) |
| `duckdb`     | off     | DuckDB analytical read engine (bundled) + typed reads |
| `datafusion` | off     | DataFusion analytical read engine (pure-Rust) + typed reads |

`duckdb` and `datafusion` currently share an Arrow version and may be enabled
together.

## License

MIT
