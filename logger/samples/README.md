# SyncLite Logger — Java Samples

This folder contains the Java samples for the SyncLite Logger.

## Samples

- `SyncliteDeviceApp.java`
- `SyncLiteStoreDeviceApp.java` (replaces previous Appender sample)
- `SyncLiteStreamingApp.java`
- `SyncLiteStoreAPIApp.java`
- `SyncLiteStreamAPIApp.java`
- `SyncLiteKafkaProduceApp.java`
- `SyncLiteJedisAPIApp.java`

## Build & Run

Compile against the built logger jar:

```
javac -cp ..\target\synclite-oss.jar *.java
```

Run any sample (example):

```
java -cp ..\target\synclite-oss.jar;. SyncliteDeviceApp
```

Notes:
- Samples default to SQLite and include inline comments for replacing SQL-device / appender device types.
- Keep `synclite.conf` in the current working directory.

## Python users

Python does not use this Java logger. Python consumes SyncLite through the
Rust runtime and its PyO3 bindings (`synclite` package). Samples live in
[`synclite-code-samples/synclite-logger/python/`](../../../../synclite-code-samples/synclite-logger/python/).
