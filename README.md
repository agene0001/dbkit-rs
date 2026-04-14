# dbkit-rs

Reusable Postgres + DuckDB database infrastructure for Rust applications.

## Features

- **Connection pooling** — `ConnectionManager` wraps deadpool-postgres with auto-create database, configurable pool size, and timeouts
- **Configurable** — `DbkitConfig` builder with connection string construction, SSL modes, and pool tuning
- **Unified query executor** — `BaseHandler` for Postgres writes (`WriteOp`) and optional DuckDB analytical reads (`ReadOp`)
- **Arrow support** — `execute_arrow()` returns `Vec<RecordBatch>` for ML/analytics pipelines
- **PG→DuckDB sync** — `sync_tables()` and `sync_table_filtered()` copy Postgres data into local DuckDB for fast analytical reads
- **Migration tracking** — `InitializationHandler` with named migrations tracked by content hash
- **Concurrent cache** — Generic DashMap-based key-value cache with named buckets (keys and values default to `String`)
- **Unicode normalization** — `BaseHandler::normalize_name()` for consistent name matching via NFD decomposition
- **Optional DuckDB** — behind a `duckdb` feature flag to avoid the heavy bundled build when not needed

## Usage

```rust
use dbkit::{ConnectionManager, DbkitConfig, BaseHandler, InitializationHandler};
use std::sync::Arc;

// Connect with defaults
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

let pool = Arc::new(conn.pool().clone());

// Run tracked migrations
let init = InitializationHandler::new(pool.clone());
init.run_named_migration("001_users", "
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    )
").await?;

// Query
let handler = BaseHandler::new(pool);
```

### Pool health

```rust
let status = conn.pool_status();
println!("connections: {}/{}", status.size, status.max_size);
println!("available: {}, waiting: {}", status.available, status.waiting);
```

### DuckDB reads (optional)

Enable the `duckdb` feature:

```toml
dbkit = { version = "0.2", features = ["duckdb"] }
```

#### Standard mapped reads

```rust
use dbkit::{BaseHandler, ReadOp, DuckParam, FetchMode};

let handler = BaseHandler::with_duckdb(pool, "postgres://localhost/myapp")?;

let result = handler.execute_read(ReadOp::Standard {
    query: "SELECT name FROM users WHERE id = $1",
    params: vec![DuckParam::Int(1)],
    map_fn: |row| Ok(row.get::<_, String>(0)?),
    mode: FetchMode::One,
}).await?;
```

#### Arrow reads

Returns `Vec<RecordBatch>` directly — ideal for ML training pipelines or columnar analytics:

```rust
use dbkit::RecordBatch;

let batches = handler.execute_arrow(
    "SELECT * FROM training_data WHERE label = $1",
    &[DuckParam::Text("positive".into())],
).await?;
```

#### Optional parameters

Use `Opt*` variants when values may be `NULL`:

```rust
let params = vec![
    DuckParam::OptInt(Some(42)),
    DuckParam::OptText(None),        // binds as NULL
    DuckParam::OptBool(Some(true)),
];
```

#### Syncing tables from Postgres to DuckDB

Copy full tables or filtered subsets into DuckDB local memory for fast analytical queries:

```rust
// Sync entire tables
handler.sync_tables(&["users", "orders", "products"]).await?;

// Sync with a filter
handler.sync_table_filtered(
    "orders",
    "created_at > $1",
    &[DuckParam::Text("2024-01-01".into())],
).await?;

// Now query the local copy (memory.main.orders) for fast reads
let batches = handler.execute_arrow(
    "SELECT * FROM memory.main.orders",
    &[],
).await?;
```

### Name normalization

Unicode NFD decomposition + lowercase for consistent name matching:

```rust
let normalized = BaseHandler::normalize_name("José García");
assert_eq!(normalized, "jose\u{301} garci\u{301}a");
```

### Cache

Both key and value types are generic, defaulting to `String`:

```rust
use dbkit::Cache;

// Default String/String cache — works exactly like before
let cache: Cache = Cache::with_buckets(&["products", "prices"]);
cache.set("products", "abc123".into(), "Widget".into());
let val = cache.get("products", &"abc123".into());

// Typed values: String keys, i32 values
let counts: Cache<String, i32> = Cache::with_buckets(&["metrics"]);
counts.set("metrics", "page_views".into(), 42);
assert_eq!(counts.get("metrics", &"page_views".into()), Some(42));

// Typed keys and values: i32 keys, custom struct values
#[derive(Clone)]
struct User { name: String }

let users: Cache<i32, User> = Cache::new();
users.set("active", 1, User { name: "Alice".into() });
let user = users.get("active", &1).unwrap();
```

## Feature flags

| Feature  | Default | Description |
|----------|---------|-------------|
| `duckdb` | off     | Enables DuckDB reads, Arrow support, and PG→DuckDB sync via `BaseHandler::with_duckdb()` |

## License

MIT
