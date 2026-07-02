# Migrating between dbkit versions

## 0.4 → 0.5 (quick checklist)

0.5 is a correctness release; most code compiles unchanged. Check these four:

1. **Custom `ReadEngine` implementations** must add the new `drop_table`
   method (the built-in DuckDB/DataFusion engines already have it).
2. **Exhaustive `match` on `DbkitError`** needs an arm for the new
   `InvalidArgument` variant.
3. **`Any`-pool NULLs are now text-typed.** A bare `DbValue::Null` bound into
   a *non-text* Postgres column now needs an explicit cast in the SQL
   (`$1::bigint`) — the same rule that already applied to the rich-type text
   fallback. (`PgHandler` is unaffected; its `NULL` stays typeless.) In
   exchange, NULLs into text/varchar columns — which previously *failed* —
   now work.
4. **`ConfigBuilder` URLs always carry an explicit ssl parameter** now, and
   MySQL gets the spelling sqlx-mysql actually parses. If you depended on
   `SslMode::Disable` (the default) silently behaving as the driver default
   (*prefer*), set `SslMode::Prefer` explicitly.

Also note: migration hashes recorded by 0.4 and earlier are upgraded in place
the first time `run_named_migration` sees the same content again — no action
needed, but don't hand-edit `_dbkit_migrations`.

# Migrating from dbkit 0.2 to 0.3

## The one-sentence shift

0.2 was **Postgres-only** (deadpool + tokio-postgres) with a DuckDB read side
bolted on. 0.3 is **multi-backend**: writes go through [sqlx] (Postgres / MySQL /
SQLite, selected by the connection-URL scheme), reads go through a pluggable
**Arrow** engine (DuckDB or DataFusion), and a `postgres-native` feature gives
you back full Postgres types when you need them.

[sqlx]: https://docs.rs/sqlx

## Connection

| 0.2 | 0.3 |
|---|---|
| deadpool `Pool`, tokio-postgres `Config` | `ConnectionManager` over sqlx; auto-creates the database |
| one Postgres pool | `pool()` → `AnyPool` (multi-backend, basic scalars only) **and** `pg_native_pool()` → native `sqlx::PgPool` (rich types) |

```rust
let conn = ConnectionManager::connect(config).await?;
let any  = conn.pool();                  // sqlx AnyPool — portable, basic scalars
let pg   = conn.pg_native_pool().await?; // sqlx PgPool — full Postgres types
```

The URL scheme picks the backend (`postgres://`, `mysql://`, `sqlite://`). If you
were a Postgres-with-rich-types user in 0.2, you want the **native pool**.

## Handlers

- **`BaseHandler`** — wraps the `Any` pool. Portable across backends, but can only
  carry basic scalar types (bool / int / float / text / bytes).
- **`PgHandler`** (`postgres-native` feature) — wraps a native `PgPool`, binds the
  *rich* types (date / timestamp / json / uuid / time / arrays) and returns
  `PgRow`. **This is the 0.2-equivalent handler** for a Postgres app.

## Parameters — the unifying change

This is the biggest day-to-day difference.

| 0.2 | 0.3 |
|---|---|
| writes: `&[&dyn ToSql]` (`Box::new(x)`) | **one `DbValue` enum** for everything |
| reads: a separate `DuckParam` enum | the same `DbValue` |

`DbValue` has `From` impls for the usual types — including `Option<T>` (→ `Null`),
and, with `postgres-native`, dates / timestamps / json / uuid / time / arrays — so
the call-site idiom becomes `x.into()`:

```rust
// 0.2
vec![Box::new(player_id), Box::new(name)]            // writes (ToSql)
vec![DuckParam::Int(team_id), DuckParam::Text(s)]    // reads

// 0.3
vec![player_id.into(), name.into()]                  // both
```

A `DbValue::Null` is bound as a *typeless* parameter (Postgres OID 0), so it
unifies with any column type in `COALESCE` / `CASE` instead of being pinned to one
type.

## Writes — unchanged in spirit

```rust
handler.execute_write(WriteOp::Single { query, params, mode }).await?;
```

`WriteOp::Single` / `BatchDDL` / `BatchParams` are the same self-documenting enum
as 0.2 — only the params type changed (`ToSql` → `DbValue`). `BatchParams` now
takes an `isolate_rows: bool`: with `true` (the 0.2 behavior) `PgHandler` wraps
each row in a `SAVEPOINT` and binds it non-persistently, so one failing row rolls
back on its own instead of aborting (and silently sinking) the whole batch. With
`false` the batch runs all-or-nothing without savepoints — ~2× faster, for
trusted bulk inserts (prefer `PgHandler::copy_in` for the fastest plain load).

## Reads — this is what changed most

0.2 had one closure-based entry point that returned a union you then unwrapped:

```rust
// 0.2
let res = handler.execute_read(ReadOp::Standard {
    query, params, map_fn: |row| row.get(0), mode,
}).await?;
let v = res.standard()?.optional()?;
```

0.3 **removes** that. The `map_fn` closure was tied to a concrete `duckdb::Row`,
which can't work once the read engine is swappable. It's replaced by methods with
exact return types:

| 0.2 | 0.3 |
|---|---|
| `ReadOp::Standard { map_fn }` (DuckDB, mapped rows) | `execute_read_as::<T: Deserialize>(sql, params) -> Vec<T>` (typed via `serde_arrow`) |
| `ReadOp::Arrow` | `execute_read(sql, params) -> Vec<RecordBatch>` |
| *(new)* native Postgres rows | `PgHandler::query(sql, params, mode) -> QueryResult<PgRow>` |

```rust
// 0.3 — typed analytical read
#[derive(serde::Deserialize)]
struct Row { name: String, qty: i64 }
let rows: Vec<Row> = handler.execute_read_as("SELECT name, qty FROM items", &[]).await?;

// 0.3 — native Postgres lookup
let res = handler.query("SELECT id FROM teams WHERE name = $1",
                        vec![name.into()], FetchMode::Optional).await?;
let id = res.optional()?.map(|r| r.get::<i32, _>(0));
```

**Why methods instead of one read enum?** The read variants return *different*
types (rows vs Arrow batches vs typed structs), so a single enum would force a
union return type plus a runtime `.unwrap` step — which is exactly what 0.2's
`ReadResult.standard()?` was. Writes kept the enum because every `WriteOp`
variant returns the same `QueryResult<PgRow>`.

A useful routing rule: small OLTP/key lookups → `query()` (Postgres); large
scans / aggregations → `execute_read` / `execute_read_as` (the analytical engine).

## Result types

`QueryResult<T>` with `.one()` / `.optional()` / `.all()` is unchanged. New: Arrow
`RecordBatch` (re-exported as `dbkit::RecordBatch`) is the read contract for the
analytical path.

## Migrations

`InitializationHandler::new(pool: AnyPool, backend: Backend)` is now
backend-aware — it emits portable tracking-table DDL, placeholders, and casts.
Get the backend from `ConnectionManager::backend()`. Your migration SQL is still
written for the database you connected to.

## Features

- `postgres-native` — native `PgPool` + `PgHandler` + rich `DbValue` types.
- `duckdb` / `datafusion` — embedded Arrow read engines (both on the same arrow
  major; may be enabled together).
- `clickhouse` — remote analytical source/sink.

## sqlx strictness — gotchas coming from 0.2

tokio-postgres and DuckDB were lenient about types; **sqlx is strict**. Things to
watch when porting 0.2 code:

- **Exact numeric width:** Postgres `INTEGER` → `i32` (not `i64`), `REAL` → `f32`,
  `DOUBLE PRECISION` / `FLOAT` → `f64`. A mismatch surfaces as `22P03`
  ("incorrect binary data format"). Cast in SQL (`x::bigint`) if a struct field
  needs a wider type.
- **Nullable columns** must be read as `Option<T>` (or via `try_get`), or `get`
  panics on `NULL`.
- **Dates** should be bound as native `DbValue::Date`, not a text param —
  comparing a text parameter to a `DATE` column has no implicit cast.
- **Placeholders differ:** native Postgres uses `$1, $2, …`; DuckDB uses `?`.
- **Credentials in a built URL** are percent-encoded, so special characters in a
  user/password (e.g. `?`, `@`, `:`) no longer corrupt the connection string.

## Cheat-sheet

| 0.2 | 0.3 |
|---|---|
| `Box::new(x)` / `DuckParam::X(x)` | `x.into()` (→ `DbValue`) |
| `execute_write(WriteOp::…)` | same (params are `DbValue`) |
| `execute_read(ReadOp::Standard{map_fn})` | `execute_read_as::<T>(sql, params)` *or* `query(sql, params, mode)` |
| `execute_read(ReadOp::Arrow)` | `execute_read(sql, params)` |
| `ReadResult::standard()?` / `.arrow()?` | (gone — direct return types) |
| deadpool `Pool` | `AnyPool` / `PgPool` |
| `InitializationHandler::new(pool)` | `InitializationHandler::new(pool, backend)` |
