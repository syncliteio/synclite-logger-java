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

## Build & Run

Compile against the built logger jar:

```
javac -cp ..\logger\target\synclite-oss.jar *.java
```

Run any sample (example):

```
java -cp ..\logger\target\synclite-oss.jar;. SyncliteDeviceApp
```

`SyncliteSqlitePostgresApp` uses the in-process consolidator, but the same `synclite-oss.jar` above already bundles it — nothing extra to add on the classpath:

```
java -cp ..\logger\target\synclite-oss.jar;. SyncliteSqlitePostgresApp
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
[`synclite-code-samples/synclite-runtime/python/`](../../synclite-code-samples/synclite-runtime/python/).
