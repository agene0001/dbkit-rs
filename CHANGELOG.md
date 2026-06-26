# Changelog

All notable changes to this project are documented here.

## [0.3.1]

Additive release (no breaking changes). Restores a native-Postgres,
rich-typed handler and the ergonomic row-mapped read API from 0.2.x, so
applications with pervasive Postgres-specific types and closure-mapped reads can
adopt 0.3 by aliasing rather than rewriting.

### Added

- **`PgHandler`** (`postgres-native` feature) — a rich-typed counterpart to
  `BaseHandler` backed by a native `sqlx::PgPool`. Binds date/timestamp/json/
  uuid/time/array `DbValue`s to their native Postgres types (no text fallback)
  and returns `PgRow`s. `new` / `with_duckdb` / `with_duckdb_attached_postgres`
  constructors; `execute_write` (sqlx) and `execute_read` (DuckDB, row-mapped).
- **Row-mapped read API** — `ReadOp::Standard { query, params, map_fn, mode }`
  (closure over a native `duckdb::Row`) and `ReadOp::Arrow`, returning
  `ReadResult` (`.standard()` / `.arrow()`). Driven by `PgHandler::execute_read`.
  Restores the 0.2.x ergonomics alongside the Arrow-only `execute_read`.
- **Rich `DbValue` variants** (`postgres-native`): `Date`, `DateTime`,
  `TimestampTz`, `Json`, `Uuid`, `Time`, `TextArray`, `FloatArray`,
  `OptFloatArray`, each with a `From` conversion. Native binds via `PgHandler`;
  on the `Any` write path and DuckDB read path they fall back to a text / array-
  literal rendering (enough for filters and Postgres assignment casts).

## [0.3.0]

A major rewrite turning dbkit from a Postgres-+-DuckDB library into a
multi-backend toolkit. This release has **breaking changes** throughout the API;
see "Migrating from 0.2" below.

### Added

- **Multi-backend writes** via sqlx's `Any` driver — the backend (Postgres,
  MySQL, SQLite) is selected by the connection URL scheme (`postgres://`,
  `mysql://`, `sqlite://`).
- **`DbValue`** — a backend-neutral parameter type (with `From` conversions)
  replacing driver-specific param types on both the write and read sides.
- **Pluggable analytical read engines** behind feature flags: `duckdb` and
  `datafusion`. Both are on arrow 58 and may be enabled together. The read
  contract is Arrow `RecordBatch`.
- **`execute_read`** (Arrow batches) and **`execute_read_as::<T: Deserialize>`**
  (typed rows via `serde_arrow`), replacing the old closure-mapped reads.
- **Generic sync** — `sync_tables` / `sync_query` copy data from the
  transactional store into the read engine (`sqlx → Arrow → engine`), working
  for any backend × engine combination.
- **`with_duckdb_attached_postgres`** — restores the zero-copy live Postgres
  query path (`SELECT … FROM pg.public.table`) via DuckDB's `ATTACH`.
- **Remote analytical sources** (`clickhouse` feature): `RemoteSource` /
  `RemoteSourceExt::query_as` for querying, and `RemoteSink` /
  `RemoteSinkExt::write_rows` for bulk Arrow writes. `ClickHouseSource`
  implements both over the HTTP interface using `FORMAT ArrowStream`.
- **Backend-aware migrations** — `InitializationHandler` now takes a `Backend`
  and emits portable tracking-table DDL, placeholders, and casts for
  Postgres/MySQL/SQLite.
- **`postgres-native` feature** — `ConnectionManager::pg_native_pool()` returns a
  native `sqlx::PgPool` with full Postgres type support (uuid/chrono/json) for
  rich types the `Any` pool cannot represent.
- Direct `arrow` re-export (`dbkit::arrow`) so callers can inspect batches with a
  version-matched arrow.

### Changed

- `ConnectionManager` is now built on sqlx `AnyPool` instead of
  deadpool-postgres; `pool()` returns an `AnyPool`.
- `BaseHandler::new` takes an `AnyPool`; `execute_write` returns
  `QueryResult<AnyRow>` (was `PgRow`).
- `WriteOp` params are `Vec<DbValue>` (was `&[&dyn ToSql]`).
- `PoolStatus` exposes `idle` instead of `waiting`.
- Migrated to sqlx 0.9 (dynamic SQL now goes through `AssertSqlSafe`).
- Crate/lib name is now `dbkit` (was `dbkit_rs`).

### Removed

- `deadpool-postgres` / `tokio-postgres` dependencies and the `ReadOp` /
  `DuckParam` / `ReadResult` read API.

### Migrating from 0.2

- Imports change from `dbkit_rs::` to `dbkit::`.
- Replace `&[&dyn ToSql]` params with `Vec<DbValue>` (`vec![x.into(), …]`).
- Replace `ReadOp::Standard { map_fn, … }` with `execute_read_as::<T>`.
- `InitializationHandler::new(pool)` → `new(pool, backend)`.
- **Rich Postgres types**: the multi-backend `Any` pool only supports basic
  scalars (bool/int/float/text/bytes). For `uuid`/`timestamptz`/`jsonb`/array/
  decimal columns, enable `postgres-native` and use
  `ConnectionManager::pg_native_pool()`.

## [0.2.1]

- Initial published release: Postgres + DuckDB infrastructure with connection
  pooling, caching, batch operations, and migrations.
