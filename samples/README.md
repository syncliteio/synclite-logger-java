# SyncLite Logger — Java Samples

This folder contains the Java samples for the SyncLite Logger.

## Samples

- `SyncliteDeviceApp.java` — plain SQLite logger
- `SyncliteSqlitePostgresApp.java` — in-process consolidator -> PostgreSQL
- `SyncLiteStoreDeviceApp.java` (replaces previous Appender sample)
- `SyncLiteStreamingApp.java`
- `SyncLiteStoreAPIApp.java`
- `SyncLiteStreamAPIApp.java`
- `SyncLiteKafkaProduceApp.java`
- `SyncLiteJedisAPIApp.java`

## Device families

The samples span three **device families**. Pick the one that matches how
your app wants to write:

- **SQL device** — a full, SQLite-syntax-compliant embedded SQL database
  reached through the JDBC `Connection` API. Run arbitrary
  `CREATE` / `ALTER` / `SELECT` / `INSERT` / `UPDATE` / `DELETE`. Reach for
  it when your app needs real SQL, JOINs, multi-statement transactions, or
  ad-hoc DDL. See `SyncliteDeviceApp.java` and
  `SyncliteSqlitePostgresApp.java`.
- **Store device** — tuned for bulk write-through. The runtime emits
  pre-formed row events that the Consolidator applies directly to the
  destination — no SQL-log parsing or CDC-deduction on the apply path — so
  it delivers the highest end-to-end consolidation throughput, and is
  usually the fastest *and* simplest starting point for a new app. Drive
  it as a SQL device (`SyncLiteStoreDeviceApp.java`) or through the typed
  `SyncLiteStore` CRUD API — `insert` / `insertBatch` / `selectAll` over
  plain `Map`s with automatic schema evolution (`SyncLiteStoreAPIApp.java`).
- **Streaming device** — append-only ingestion for high-throughput event
  capture; accepts inserts and rejects update / delete by design. Drive it
  as a device (`SyncLiteStreamingApp.java`) or through the fluent
  `SyncLiteStream` `insert` / `insertBatch` API (`SyncLiteStreamAPIApp.java`).

All three produce the same change log and flow through the same shipper +
consolidator, so you can mix device families inside one application.

## Build & Run

Compile against the built logger jar:

```
javac -cp ..\logger\target\synclite-1.0.0.jar *.java
```

Run any sample (example):

```
java -cp ..\logger\target\synclite-1.0.0.jar;. SyncliteDeviceApp
```

`SyncliteSqlitePostgresApp` uses the in-process consolidator, but the same `synclite-1.0.0.jar` above already bundles it — nothing extra to add on the classpath:

```
java -cp ..\logger\target\synclite-1.0.0.jar;. SyncliteSqlitePostgresApp
```

Notes:
- Samples default to SQLite and include inline comments for replacing SQL-device / appender device types.
- Keep `synclite.conf` in the current working directory.

## Python users

Python does not use this Java logger. Python consumes SyncLite through
the Rust runtime via the
[`synclite`](../../synclite-logger-rust/python/) PyO3 wheel — build it
from source with `maturin develop --release` in
`synclite-logger-rust/python/` (a PyPI release is on the roadmap).
Samples live in
[`synclite-code-samples/python/`](../../synclite-code-samples/python/).
