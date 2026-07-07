# Changelog

All notable changes to this project are documented here.

## [0.5.1]

### Fixed

- **`normalize_name` is now genuinely accent-insensitive.** It NFD-decomposed
  and lowercased but kept the combining marks, so it only equalized composed
  vs decomposed representations of the *same* accented string — "José Ramírez"
  never matched "Jose Ramirez", despite the function's documented purpose.
  Since US data feeds routinely strip diacritics while league-official sources
  keep them, name-keyed matching missed on every accented name (verified
  downstream: 25+ split player identities, and name-only lookups failing for
  accent-stripped inputs). Combining marks are now dropped after decomposition
  (NFD → strip marks → lowercase), in both `BaseHandler` and `PgHandler`.
  Callers persisting normalized keys should regenerate them, since the key for
  accented names changes.

## [0.5.0]

A correctness-and-performance release: four bug fixes that could bite silently
in production, several semantic hardening changes, and measured speedups on the
COPY render path. Behavior changes are called out below.

### Fixed

- **`ConfigBuilder::ssl_mode` with `Backend::MySql` produced a URL sqlx rejects
  at connect time.** The builder wrote the Postgres spelling
  (`?sslmode=prefer|require`), but sqlx-mysql only accepts
  `ssl-mode=DISABLED|PREFERRED|REQUIRED` — every MySQL connection with a
  non-default `ssl_mode` failed with a configuration error. The parameter is
  now rendered per backend.
- **Migration hashes are now a stable FNV-1a 64** instead of std's
  `DefaultHasher`, whose algorithm is explicitly not guaranteed across Rust
  releases — a toolchain upgrade could have made every already-applied
  migration error with "content has changed". Rows recorded by older dbkit
  versions are recognized via the legacy hash and upgraded in place on the next
  `run_named_migration` with unchanged content.
- **`DbValue::Null` on the `Any` pool binds as a *text* NULL** (was
  `Option::<i64>::None`, i.e. an `int8`-typed NULL on Postgres, so a NULL into
  a varchar/text column failed with "column is of type X but expression is of
  type bigint"). Text matches the `Any` path's existing text-fallback strategy
  for rich types: NULL and non-NULL rows now behave the same for a given
  column. **Behavior change:** a bare NULL into a *non-text* Postgres column
  now needs an explicit cast in SQL (`$1::bigint`), the same rule as the
  rich-type text fallback; use `PgHandler` for typeless NULL inference.
  NULL-bearing statements also re-parse (`persistent(false)`) so they can't
  poison the cached prepared statement's parameter types — the same 22P03
  guard `PgHandler` gained in 0.4.2.
- **`BaseHandler` `BatchParams { isolate_rows: true }` no longer silently loses
  the whole batch on Postgres.** Without savepoints, the first bad row aborted
  the transaction, every later row failed with 25P02, and the final `COMMIT`
  was silently converted to `ROLLBACK` — zero rows committed, yet the call
  returned `Ok`. The multi-backend path now wraps each row in a `SAVEPOINT`
  (standard SQL: Postgres, MySQL/InnoDB, SQLite), giving real per-row
  isolation, matching `PgHandler`.
- **`sync_query` / `sync_tables` of an empty result no longer leaves the
  previously synced table serving stale rows.** Zero rows now *drop* the
  analytical table (the schema can't be inferred from an empty `Any` result
  set), so reads fail with "table not found" instead of silently returning old
  data.
- **`copy_upsert` staging no longer burns target sequences.** The staging table
  is now `CREATE TEMP TABLE … AS SELECT {columns} FROM target WITH NO DATA`
  (only the copied columns, no constraints, no defaults) instead of
  `LIKE target INCLUDING DEFAULTS`, which made COPY fire a copied
  serial/`nextval()` default once per staged row.
- **`DuckEngine::attach_postgres` escapes single quotes in the connection
  string** (common in passwords), which previously broke out of the `ATTACH`
  literal. Table names in `load_table` are likewise quote-escaped.
- **`copy_upsert` with empty `conflict_columns`** now fails up front with
  `DbkitError::InvalidArgument` instead of an opaque Postgres syntax error.

### Performance

Measured against local Postgres 18 with `bench/` (end-to-end, median of 3
runs, interleaved old/new binaries) and `bench/src/bin/micro.rs` (client-side
CPU cost in isolation, no network):

- **COPY text rendering** — `copy_escape_into` copies clean spans in bulk
  instead of pushing char-by-char; a cell with no escapable bytes (the common
  case) is one `push_str`. Isolated: **2.2× faster** on clean cells
  (17.0 → 7.6 ns/cell), neutral (1.00×) on escape-heavy cells. End-to-end
  COPY throughput is network/server-bound at these cell sizes (~0.5M rows/s
  for 200k rows), so wall-clock is unchanged — this buys CPU headroom, not
  latency.
- **COPY payloads stream in ~4 MiB chunks** instead of materializing the whole
  payload: peak memory is bounded regardless of row count (a 10M-row load no
  longer holds the entire rendered payload in RAM), and rendering overlaps
  with the network sends. Same end-to-end throughput at bench sizes.
- **`PgHandler` binds text/bytes/json/array parameters by reference** — no
  per-value clone per row. Isolated bind cost drops **17%**
  (353 → 292 ns/row for a 3-column row); `BatchParams` end-to-end is
  round-trip-bound, so this is CPU headroom under concurrency.
- **Per-row statement-reuse granularity in the `BatchParams` fast path**: only
  rows that actually contain a NULL re-parse; null-free rows keep reusing the
  cached prepared statement (was: one NULL anywhere disabled reuse for the
  whole batch). The per-row scan costs ~0.4 ns/row.
- **DuckDB reads no longer serialize behind one Mutex** — each `query_arrow`
  runs on a cheap `try_clone`d connection to the same instance (shared
  catalog), so concurrent analytical reads execute in parallel.
- **`Cache::set` no longer allocates** a `String` for the bucket name when the
  bucket already exists (the steady state).

### Changed (breaking)

- **`ReadEngine` gained a required `drop_table` method** (used by the
  empty-sync fix). External implementors must add it; the built-in DuckDB and
  DataFusion engines are updated.
- **`DbkitError` gained the `InvalidArgument` variant** (exhaustive matches
  need a new arm).
- **`ConfigBuilder::build` always writes the ssl parameter explicitly**,
  including the default `SslMode::Disable` (`?sslmode=disable` /
  `?ssl-mode=DISABLED`). Previously `Disable` wrote nothing, which silently
  fell back to the sqlx driver default — *prefer*, not disable — contradicting
  the documented "No SSL" semantics. If you relied on the accidental
  prefer-TLS behavior, set `SslMode::Prefer` explicitly.

## [0.4.2]

### Fixed

- **`PgHandler::query` (the `WriteOp::Single` path) no longer fails with 22P03
  on reused statements containing NULLs.** With the default `persistent(true)`,
  sqlx caches one prepared statement per `(connection, SQL)`; a typeless NULL
  (`PgNull`, OID 0) lets the server pin a parameter's type from the first
  execution, so a later call binding a concrete type for that same column failed
  with `22P03` ("incorrect binary data format"). `query` now binds with
  `persistent(false)` whenever a call contains a NULL (keeping caching for the
  common no-NULL case) — the same NULL-aware guard the `BatchParams` write path
  already uses. This silently broke order-dependent single-row upserts whose
  columns flip NULL ⇄ concrete across calls (e.g. sportsbook odds inserts where
  only some rows carry per-side odds).

## [0.4.1]

### Documentation

- Build docs.rs with `all-features` so the optional APIs (`postgres-native`'s
  `PgHandler` / `copy_in` / `copy_upsert`, the DuckDB/DataFusion read engines)
  appear in the published documentation. No code changes.

## [0.4.0]

### Added

- **`PgHandler::copy_upsert`** — bulk upsert via `COPY` into a temp staging table
  then one set-based `INSERT … SELECT … ON CONFLICT`. Benchmarks ~10× faster than
  row-by-row `BatchParams` with `ON CONFLICT` (and ~16× faster than the 0.2
  equivalent). For the cases plain `copy_in` can't cover because `COPY` is not an
  `INSERT` (no `ON CONFLICT`/`RETURNING`).

### Performance

- **`copy_in` render path** — pre-size the payload buffer, format integers/floats
  directly into it (no per-cell `String`), and write `bytea` hex without per-byte
  allocation. Shared with `copy_upsert` via a common renderer.

### Changed (breaking)

- **`WriteOp::BatchParams` gained an `isolate_rows: bool` field.** All call sites
  must now set it. Use `isolate_rows: true` to keep 0.3.x behavior (per-row error
  isolation: `PgHandler` wraps each row in a `SAVEPOINT` and skips bad rows; the
  `Any` pool warns and continues).
  - `isolate_rows: false` is a new **all-or-nothing** fast path: no per-row
    savepoints, so the first error rolls back the whole batch, but it is ~2×
    faster. On `PgHandler` it also reuses a single prepared statement
    (`persistent(true)`) when the batch contains no typeless `NULL`s, falling back
    to per-row parsing otherwise to avoid `22P03`. Use it for trusted bulk
    inserts where partial success isn't needed; prefer `PgHandler::copy_in` for
    the fastest plain bulk load.

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
