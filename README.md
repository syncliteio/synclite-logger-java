# SyncLite for Java — Embeddable Sync-Ready JDBC Driver & Full Runtime

> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) — Build Anything, Sync Anywhere.

## What is SyncLite for Java?

**SyncLite for Java** is an embeddable Java library (JDBC driver) that makes Java applications **sync-ready** with minimal code changes. It wraps popular embedded databases — SQLite, DuckDB, Apache Derby, H2, and HyperSQL — and transparently captures every SQL transaction into compact, binary log files. These log files are shipped in real time to a configured staging storage (local directory, SFTP, Amazon S3, MinIO, Apache Kafka, OneDrive, Google Drive, NFS, and more) and consolidated into the destination database, data warehouse, or data lake of your choice.

The result: edge, desktop, or mobile Java apps that work fully offline with a local embedded database and automatically replicate all changes to the cloud — without writing a single line of replication code.

## Two Deployment Modes

The Java SDK ships **two consumer jars** so you can pick the topology that matches your operational model. Both jars share the same JDBC / Store / Stream / Jedis / Kafka APIs — the difference is only *where the consolidator runs*.

### Mode 1 — Logger only (centralized consolidator)

Use `synclite-<version>.jar` (pure Java, no native libs). Your app embeds the **logger + segment shipper** and pushes segments to staging. A **standalone [SyncLite Consolidator](../synclite-consolidator/) WAR** running on your central host drains those segments and applies them to the destination.

```text
+----------------+   *.sqllog   +-----------+   apply   +-------------+
|  Java app +    | -----------> |  Staging  | <-------- | Standalone  |
|  synclite jar  |              |  (FS/S3/  |           | Consolidator|
+----------------+              |   …)      |           |     WAR     |
                                +-----------+           +-------------+
                                                              |
                                                              v
                                                        Destination DB
```

Good when: many edge/desktop devices fan in to one central pipeline; you want one place to monitor jobs.

### Mode 2 — Full embedded runtime (in-process consolidator)

The same `synclite-<version>.jar` already bundles the consolidator engine via the JNI-loaded `synclite_jni` native (and a `duckdb` shared library for DuckDB destinations). To use it as a full runtime instead of pure logger-only, call `initialize(dbPath, deviceName, destinationOptions)` instead of `initialize(dbPath, conf)`. **Logger, shipper, and consolidator now run inside your JVM** — drop in one jar, point it at a destination, and you have an offline-first app that syncs in the background.

```text
+--------------------------------------------------+
|  Your Java app                                   |
|     |                                            |
|     v                                            |
|  JDBC / Store / Stream API                       |
|     +--> local SQLite/DuckDB/Derby/H2 file       |
|     +--> *.sqllog segments                       |
|              |                                   |
|              v                                   |
|         shipper (in-JVM)                         |
|              |                                   |
|              v                                   |
|         stage (FS/S3/SFTP/…)                     |
|              |                                   |
|              v                                   |
|         embedded consolidator (JNI -> Rust)      |
|              |                                   |
|              v                                   |
|         destination apply (SQLite/DuckDB/PG)     |
+--------------------------------------------------+
```

Good when: single-process deployment (containers, microservices, agent loops), or when you want the simplest possible "one jar, sync to PostgreSQL" story — no separate service to deploy.

See [`samples/SyncliteSqlitePostgresApp.java`](samples/SyncliteSqlitePostgresApp.java) for a full embedded-runtime sample with `SQLite.initialize(dbPath, deviceName, destination)` + `SyncLite.awaitSync(...)`.

## Other Language Runtimes

For non-JVM stacks, the same SyncLite Runtime story is also available as a [Rust crate](../synclite-logger-rust/) with bindings for **Python**, **C/C++**, and any language that can call a C ABI. Java and Rust runtimes interoperate (both write the same `.sqllog` format) so you can mix them on the same staging storage.

```
Your App  +  SyncLite Logger  +  Embedded DB
     │
      v  (SQL log files)
  Staging Storage  (local / SFTP / S3 / MinIO / Kafka / OneDrive / …)
     │
      v
  SyncLite Consolidator
     │
      v
  Destination DB / Data Warehouse / Data Lake
```

## Device Types

SyncLite devices fall into three primary categories. Wherever the docs talk about "devices" use this classification:

- **SQL Devices** — Full SQL-compatible embedded databases (SQLite, DuckDB, Derby, H2, HyperSQL). Use these when your app needs the full SQL surface (arbitrary queries, DDL, DML). The replication flow for SQL devices captures SQL/command logs which the Consolidator later processes into CDC-like records for destinations.

- **Store Devices** — CRUD-focused store variants (e.g. `SQLITE_STORE`, `DUCKDB_STORE`, `DERBY_STORE`, `H2_STORE`, `HYPERSQL_STORE`) exposing the `SyncLiteStore` API. Store devices provide typed `insert` / `update` / `delete` / `selectAll` methods, automatic schema evolution, and logs that are applied directly to destinations without the two-step deduce-and-apply processing required for general SQL devices.

- **Streaming Device** — The `STREAMING` device models append-only ingestion with `SyncLiteStream` semantics (fluent `insert` / `insertBatch`). It is optimized for high-throughput event capture and does not support UPDATE/DELETE semantics.

Note: internal device types such as Appender and DBLogger remain implementation details and are intentionally omitted from user-facing documentation.
## Quick Start

### End-to-end PostgreSQL sample (30 seconds)

Drop in **one jar** (`synclite-<version>.jar` — it already bundles the in-process consolidator), point at a destination, write SQL. The snippet below opens a local SQLite device, writes to it through plain JDBC, waits for the in-process consolidator to apply the changes to PostgreSQL, then reads the row back from PostgreSQL to prove end-to-end delivery. No Consolidator WAR, no separate process.

```java
import io.synclite.*;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;

public class SyncliteSqlitePostgresApp {
    private static final Path  DB_PATH        = Path.of("orders.db");
    private static final String DEVICE_NAME   = "orders-device";
    private static final String POSTGRES_URL  = "jdbc:postgresql://localhost:5432/syncdb";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PWD  = "postgres";
    private static final String POSTGRES_DB   = "syncdb";
    private static final String POSTGRES_SCHEMA = "syncschema";

    public static void main(String[] args) throws Exception {
        // 1. One call wires up the local SQLite logger + segment shipper +
        //    embedded consolidator that drains into PostgreSQL.
        DestinationOptions dst = DestinationOptions.builder()
                .dstType(DstType.POSTGRES)
                .connectionString(POSTGRES_URL)
                .database(POSTGRES_DB)
                .schema(POSTGRES_SCHEMA)
                .syncMode(DstSyncMode.CONSOLIDATION)
                .build();
        SQLite.initialize(DB_PATH, DEVICE_NAME, dst);

        try {
            // 2. From here on, talk to a plain local SQLite database.
            try (Connection conn = DriverManager.getConnection(
                    "jdbc:synclite_sqlite:" + DB_PATH);
                 Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS orders");
                s.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT, qty INTEGER)");
                s.execute("INSERT INTO orders VALUES(1, 'widget', 100)");
            }

            // 3. Block until the in-flight segment has been applied to PostgreSQL.
            SyncLite.awaitSync(DB_PATH, Duration.ofSeconds(30));
            System.out.println("[SYNC] awaitSync succeeded");

            // 4. Read the row back from PostgreSQL to prove end-to-end delivery.
            try (Connection pg = DriverManager.getConnection(
                    POSTGRES_URL, POSTGRES_USER, POSTGRES_PWD);
                 PreparedStatement ps = pg.prepareStatement(
                    "SELECT row_to_json(t)::text FROM (SELECT * FROM "
                  + POSTGRES_SCHEMA + ".orders WHERE id = ?) t")) {
                ps.setLong(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    System.out.println("[READ FROM POSTGRESQL POST SYNC] "
                            + (rs.next() ? rs.getString(1) : "no row"));
                }
            }
        } finally {
            SQLite.closeDevice(DB_PATH);
        }
    }
}
```

Build and run with the embedded-runtime jar on the classpath:

```sh
javac -cp synclite-logger-java/logger/target/synclite-oss.jar \
      SyncliteSqlitePostgresApp.java
java  -cp synclite-logger-java/logger/target/synclite-oss.jar:. \
      SyncliteSqlitePostgresApp
```

Prerequisites: a local PostgreSQL with database `syncdb` and schema `syncschema` (the consolidator creates tables on demand). Full runnable sample: [`samples/SyncliteSqlitePostgresApp.java`](samples/SyncliteSqlitePostgresApp.java).

For the **centralized topology** (logger-only jar + standalone Consolidator WAR), see the dependency + walkthrough below.

### 1. Add the dependency

```xml
<dependency>
    <groupId>io.synclite</groupId>
    <artifactId>synclite</artifactId>
    <version><!-- latest version --></version>
</dependency>
```

Or copy `synclite-<version>.jar` from the platform release into your project classpath.

> **DuckDB users:** the DuckDB JDBC driver is **not bundled** in the
> SyncLite jar (it ships its own ~50 MB native libraries and would
> nearly double the SyncLite jar size). If your app uses any DuckDB
> device (`DUCKDB`, `DUCKDB_STORE`, `DUCKDB_APPENDER`, …), add the
> driver to your project alongside SyncLite:
>
> ```xml
> <dependency>
>     <groupId>org.duckdb</groupId>
>     <artifactId>duckdb_jdbc</artifactId>
>     <version>1.5.2.0</version>
> </dependency>
> ```
>
> Use the exact same version SyncLite was built against (`1.5.2.0`) to
> avoid native-library version mismatches. Non-DuckDB users can ignore
> this dependency entirely.

DuckDB users must add `org.duckdb:duckdb_jdbc:1.5.2.0` themselves.

### 2. Configure `synclite.conf`

A full sample config file is provided at `logger/src/main/resources/synclite.conf`. At minimum, set:

```properties
# Where to write the local sync logs (staging directory)
local-data-stage-directory=<path/to/stage>

# Where the final destination is (can also be configured in Consolidator UI)
device-stage-type=SQLITE
```

### 3. Initialize and use in Java

```java
import io.synclite.*;
import java.nio.file.Path;
import java.sql.*;

public class MyEdgeApp {
    public static void main(String[] args) throws Exception {
        Path dbDir = Path.of(System.getProperty("user.home"), "synclite", "db");
        Path dbPath = dbDir.resolve("myapp.db");
        Path conf   = dbDir.resolve("synclite.conf");

        // Initialize SyncLite Logger with SQLite
        Class.forName("io.synclite.SQLite");
        SQLite.initialize(dbPath, conf);

        try (Connection conn = DriverManager.getConnection("jdbc:synclite_sqlite:" + dbPath)) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE IF NOT EXISTS events(id INT, payload TEXT)");
                st.execute("INSERT INTO events VALUES(1, 'hello from edge')");
            }
        }

        // All transactions are captured in log files and shipped to the stage.
        // SyncLite Consolidator will pick them up and replicate to the destination DB.
        SQLite.closeAll();
    }
}
```

### 4. Streaming / Kafka Producer API

SyncLite Logger also exposes a Kafka Producer-compatible API for high-throughput data streaming applications:

```java
Class.forName("io.synclite.Streaming");
Streaming.initialize(dbPath, conf);
try (Connection conn = DriverManager.getConnection("jdbc:synclite_streaming:" + dbPath)) {
    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO telemetry VALUES(?, ?)")) {
        ps.setInt(1, 1);
        ps.setString(2, "sensor-reading");
        ps.addBatch();
        ps.executeBatch();
    }
}
```

### 5. SyncLiteStore API — CRUD without raw SQL

**STORE devices** (`SQLITE_STORE`, `DUCKDB_STORE`, `DERBY_STORE`, `H2_STORE`, `HYPERSQL_STORE`) pair the embedded database with the high-level `SyncLiteStore` API: a typed `insert` / `update` / `delete` / `selectAll` API that eliminates boilerplate SQL, handles schema evolution (auto-adds missing columns), and logs every operation to the SyncLite replication pipeline.

```java
import io.synclite.SQLiteStore;
import io.synclite.SyncLiteStore;

Class.forName("io.synclite.SQLiteStore");
Path dbPath = Path.of("mystore.db");
SQLiteStore.initialize(dbPath, Path.of("synclite.conf"));

try (SyncLiteStore store = SQLiteStore.open(dbPath)) {
    // CREATE TABLE
    store.createTable("players", new LinkedHashMap<>(Map.of(
        "id",    "INTEGER PRIMARY KEY",
        "name",  "TEXT",
        "score", "INTEGER"
    )));

    // INSERT single + batch
    store.insert("players", Map.of("id", 1, "name", "Alice", "score", 100));
    store.insertBatch("players", List.of(
        Map.of("id", 2, "name", "Bob",   "score", 200),
        Map.of("id", 3, "name", "Carol", "score", 300)
    ));

    // UPDATE + DELETE
    store.update("players", Map.of("score", 250), Map.of("name", "Bob"));
    store.delete("players", Map.of("id", 3));

    // SELECT (local read — not replicated)
    List<Map<String, Object>> rows = store.selectAll("players");
}
SQLiteStore.closeDevice(dbPath);
```

DuckDB, Derby, H2, and HyperSQL backend variants follow the same pattern — replace `SQLiteStore` / `synclite_sqlite_store` with the matching class and JDBC URL prefix.

### 6. SyncLiteStream API — Fluent Append-Only Ingestion

`SyncLiteStream` provides a fluent `insert` / `insertBatch` / `createTable` / `dropTable` API over a `STREAMING` device. UPDATE and DELETE are intentionally absent: this API models event flow, not mutable records.

```java
import io.synclite.Streaming;
import io.synclite.SyncLiteStream;

Class.forName("io.synclite.Streaming");
Path dbPath = Path.of("events.db");
Streaming.initialize(dbPath, Path.of("synclite.conf"));

try (SyncLiteStream stream = SyncLiteStream.open(dbPath)) {
    stream.createTable("events", new LinkedHashMap<>(Map.of(
        "ts",         "BIGINT",
        "event_type", "TEXT",
        "user_id",    "TEXT"
    )));

    // Single insert
    stream.insert("events", Map.of(
        "ts",         System.currentTimeMillis(),
        "event_type", "SIGNUP",
        "user_id",    "user-10"
    ));

    // Batch insert (new column auto-added on first occurrence)
    stream.insertBatch("events", List.of(
        Map.of("ts", System.currentTimeMillis(), "event_type", "VIEW",     "user_id", "user-11", "source", "web"),
        Map.of("ts", System.currentTimeMillis(), "event_type", "PURCHASE", "user_id", "user-12", "source", "app")
    ));
}
```

### 7. Jedis (Redis-Compatible) API

`io.synclite.Jedis` is a drop-in extension of the `redis.clients.jedis.Jedis` class. Every write (strings, hashes, lists, sets, sorted sets, key expiry, delete) is **durably committed to a `SQLITE_STORE` SyncLite device** before being forwarded to Redis. On the next startup the cache is automatically repopulated from the store, so Redis data survives restarts. All captured mutations flow through SyncLite Consolidator to any downstream destination.

```java
import io.synclite.Jedis;

Path dbPath = Path.of("cache.db");

// Managed mode — Jedis handles SQLiteStore initialise / open / close
try (Jedis jedis = Jedis.builder(dbPath, Path.of("synclite.conf"), "jedis-device")
        .host("localhost").port(6379).build()) {

    // Strings
    jedis.set("user:1:name", "Alice");
    jedis.mset("user:2:name", "Bob", "user:3:name", "Carol");
    System.out.println(jedis.get("user:1:name")); // Alice

    // Hashes
    jedis.hset("session:42", Map.of("token", "abc123", "status", "active"));
    System.out.println(jedis.hgetAll("session:42"));

    // Lists
    jedis.rpush("queue", "job-1", "job-2");
    jedis.lpush("queue", "job-0");
    System.out.println(jedis.lrange("queue", 0, -1)); // [job-0, job-1, job-2]

    // Sets
    jedis.sadd("tags", "etl", "cdc", "ops");
    jedis.srem("tags", "ops");

    // Sorted sets
    jedis.zadd("leaderboard", Map.of("Alice", 100.0, "Bob", 200.0));
    System.out.println(jedis.zrangeWithScores("leaderboard", 0, -1));

    // Key expiry + delete
    jedis.setex("tmp", 60, "ephemeral");
    jedis.del("tmp");
}
```

The `Jedis.builder(SyncLiteStore)` overload is available when the application manages the store lifecycle externally. See `samples/SyncLiteJedisAPIApp.java` for the full sample.

## Non-Java Runtimes

The Java SDK is JVM-only. For **Python, C/C++, Go, Ruby, Node.js, Rust** — anything that can call a C ABI — use the [SyncLite Rust runtime](../synclite-logger-rust/) which packages the same logger + embedded consolidator behind a C ABI plus PyO3 bindings. Python samples live in [`synclite-code-samples/synclite-logger/python/`](../synclite-code-samples/synclite-logger/python/).

## Code Samples

All Java sample applications live under [`samples/`](samples/):

```
samples/
├─ SyncliteDeviceApp.java          # Plain SQLite logger
├─ SyncliteSqlitePostgresApp.java  # In-process consolidator -> PostgreSQL
├─ SyncLiteStoreDeviceApp.java     # SyncLiteStore key/value device
├─ SyncLiteStreamingApp.java       # Streaming device
├─ SyncLiteStoreAPIApp.java        # Store API (in-memory facade)
├─ SyncLiteStreamAPIApp.java       # Stream API (in-memory facade)
├─ SyncLiteJedisAPIApp.java        # Jedis-compatible facade
└─ SyncLiteKafkaProduceApp.java    # Kafka-style producer facade
```

Python samples live under [`synclite-code-samples/synclite-logger/python/`](../synclite-code-samples/synclite-logger/python/).

## Staging Storages Supported

| Storage | Notes |
|---|---|
| Local directory | Single-host or shared NFS mount |
| SFTP | Remote file server |
| Amazon S3 | Cloud object storage |
| MinIO | Self-hosted S3-compatible |
| Apache Kafka | Message-based transport |
| Microsoft OneDrive | Enterprise cloud share |
| Google Drive | Personal/workspace cloud share |
| NFS / Network share | LAN-level sharing |

## Build

A single Maven module (`logger/`) produces a single artifact: `synclite-<version>.jar`. The same jar covers both deployment modes — when a Rust toolchain is present the build also bundles the in-process consolidator native (`synclite_jni`); when it isn't, the jar still builds cleanly and contains only the Java logger.

```bash
cd synclite-logger-java/logger
mvn -Drevision=oss clean install
```

Output: `logger/target/synclite-oss.jar`.

### Skipping the Rust build

If you don't have a Rust toolchain (or only need the logger-only jar), pass `-DskipRustRuntime=true`:

```bash
mvn -Drevision=oss -DskipRustRuntime=true clean install
```

Without this flag the build still tolerates a missing `cargo` — the cargo step and the native-bundling antrun step are both best-effort (`failOnError=false`). The flag just makes the skip explicit and silences the failure log.

### Building the Rust native explicitly

The pom invokes `cargo build -p synclite-bindings-java --release` against [synclite-logger-rust/](../synclite-logger-rust/) automatically. To pre-build (e.g. cross-compiled Linux/macOS targets), run it yourself first — the antrun step will pick up whatever's already in `synclite-logger-rust/target/`:

```bash
cd synclite-logger-rust
cargo build -p synclite-bindings-java --release
# optionally for other targets:
cargo build -p synclite-bindings-java --release --target <triple>

cd ../synclite-logger-java/logger
mvn -Drevision=oss clean install
```

The native ends up at `META-INF/native/<os-arch>/libsynclite_<rev>_jni.<ext>` inside the jar; `NativeLoader` resolves it at runtime from the classpath.

### Skipping tests

Append `-DskipTests` to skip running JUnit, or `-Dmaven.test.skip=true` to skip test compilation too:

```bash
mvn -Drevision=oss -DskipTests clean install
mvn -Drevision=oss -Dmaven.test.skip=true clean install
```

## Related Components

| Component | Role |
|---|---|
| [SyncLite DB](https://github.com/syncliteio/synclite-db) | HTTP server wrapping embedded DBs for any language |
| [SyncLite Client](https://github.com/syncliteio/synclite-client) | CLI to interact with SyncLite devices |
| [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) | Central consolidation server |

## Documentation & Community

- Full documentation: https://github.com/syncliteio/SyncLite/blob/main/DOCUMENTATION.md
- SyncLite Logger configuration reference: https://github.com/syncliteio/SyncLite/blob/main/DOCUMENTATION.md
- Community: https://github.com/syncliteio/SyncLite/issues
- Website: https://www.synclite.io

- Log format details: see the **SyncLite Log Format** section in the platform documentation: https://github.com/syncliteio/SyncLite/blob/main/DOCUMENTATION.md#synclite-log-format

---

← Back to the [SyncLite Platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)

