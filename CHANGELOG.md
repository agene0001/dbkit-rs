# Changelog

All notable changes to this project are documented here.

## [0.3.5]

### Fixed

- **`PgHandler` `WriteOp::BatchParams` now wraps each row in a SAVEPOINT.** The
  batch runs in one transaction and skips failing rows, but on Postgres the first
  row error aborts the whole transaction, so every subsequent row failed with
  25P02 ("current transaction is aborted") — one bad row sank the entire batch
  and masked the real error. Each row now gets a savepoint: a bad row rolls back
  to it (logging its *real* error) while the good rows still commit.
- **`BatchParams` rows are now bound non-persistently** (`.persistent(false)`),
  re-parsing per row instead of reusing one cached prepared statement across the
  batch. Reuse pinned each parameter's type from the first row, so a first row
  whose value was a typeless `NULL` (resolved by the server to the column type,
  e.g. `int4`) made a later row's widened integer bind (`int8`) fail with 22P03
  ("incorrect binary data format in bind parameter N"). Per-row parsing keeps
  each row's parameter types self-consistent.

## [0.3.4]

### Fixed

- **`PgHandler` now binds a `DbValue::Null` as a typeless NULL** (parameter type
  OID 0, resolved from context by the server) instead of `Option::<i64>::None`,
  which pinned every NULL to `int8`. The pinned type broke type unification —
  e.g. `COALESCE($1, external_id)` against a `varchar` column failed with
  "COALESCE types bigint and character varying cannot be matched". A typeless
  NULL behaves like a bare `NULL` literal, unifying with any column type, while
  remaining correct for `INSERT`/`UPDATE`/`WHERE`.

## [0.3.3]

### Fixed

- **`ConfigBuilder::build` now percent-encodes the user/password** when assembling
  the connection URL from parts. A raw URL-reserved character in credentials
  (e.g. `?`, `@`, `:`, `/`, `#`) previously corrupted the URL — notably a `?` in a
  password truncated the authority so the password was parsed as the port,
  yielding `error with configuration: invalid port number` and a failed
  connect/auto-create. (The 0.2 deadpool path didn't build a URL, so this only
  surfaced after the sqlx migration.)

## [0.3.2]

### Added

- **`PgHandler::execute_read_as<T>`** — typed analytical read: runs a query on
  the attached DuckDB engine and deserializes each row into `T` via `serde_arrow`
  (column names must match `T`'s fields). The typed counterpart to
  `PgHandler::execute_read`, mirroring `BaseHandler::execute_read_as`, so callers
  can keep large scans / aggregations on DuckDB without hand-writing Arrow
  column extraction.

## [0.3.1]

Additive release (no breaking changes). Adds a native-Postgres, rich-typed
handler combining an OLTP read/write path (sqlx `PgPool`) with the analytical
Arrow read path (DuckDB), so applications with pervasive Postgres-specific types
can adopt 0.3 without rewriting their reads into typed structs.

### Added

- **`PgHandler`** (`postgres-native` feature) — a rich-typed counterpart to
  `BaseHandler` backed by a native `sqlx::PgPool`. Binds date/timestamp/json/
  uuid/time/array `DbValue`s to their native Postgres types (no text fallback)
  and returns `PgRow`s. `new` / `with_duckdb` / `with_duckdb_attached_postgres`
  constructors.
- **`PgHandler::query`** — the OLTP read path: runs a query against the native
  Postgres pool and returns `QueryResult<PgRow>` per `FetchMode` (single
  round-trip, no analytical engine). Read columns with `row.get(i)` /
  `row.try_get(i)`. `execute_write`'s `Single` now delegates to it.
- **`PgHandler::execute_read`** — the analytical path: runs a query against the
  attached DuckDB engine and returns Arrow `RecordBatch`es (for large
  joins/aggregations consumed as DataFrames). Pairs with `BaseHandler`'s
  `execute_read_as` for typed deserialization.
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

See [MIGRATING.md](MIGRATING.md) for the full guide. In brief:

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
