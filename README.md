# dbkit-rs

Multi-backend database infrastructure for Rust applications.

Transactional writes go through [sqlx]'s `Any` driver — the backend (Postgres,
MySQL, or SQLite) is chosen by the connection URL scheme. Analytical reads go
through a pluggable, Arrow-based engine (DuckDB or DataFusion) behind feature
flags.

## Features

- **Multi-backend writes** — `ConnectionManager` + `BaseHandler` over sqlx; `postgres://`, `mysql://`, or `sqlite://` selects the backend
- **Connection pooling** — auto-create database, configurable pool size and timeouts
- **Configurable** — `DbkitConfig` builder with URL construction, SSL modes, and pool tuning
- **Unified writes** — `execute_write` with `WriteOp` (single / batch DDL / batch params) and neutral `DbValue` parameters
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
dbkit = { version = "0.3", features = ["duckdb"] }       # or "datafusion"
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
dbkit = { version = "0.3", features = ["postgres-native"] }
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
