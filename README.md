# SyncLite Logger — Embeddable Sync-Ready JDBC Driver

> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) — Build Anything, Sync Anywhere.

## What is SyncLite Logger?

**SyncLite Logger** is an embeddable Java library (JDBC driver) that makes any Java or Python application **sync-ready** with minimal code changes. It wraps popular embedded databases — SQLite, DuckDB, Apache Derby, H2, and HyperSQL — and transparently captures every SQL transaction into compact, binary log files. These log files are shipped in real time to a configured staging storage (local directory, SFTP, Amazon S3, MinIO, Apache Kafka, OneDrive, Google Drive, NFS, and more), where [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) continuously ingests and consolidates them into the destination database, data warehouse, or data lake of your choice.

The result: edge, desktop, or mobile apps that work fully offline with a local embedded database and automatically replicate all changes to the cloud — without you writing a single line of replication code.

```
Your App  +  SyncLite Logger  +  Embedded DB
     │
     ▼  (SQL log files)
  Staging Storage  (local / SFTP / S3 / MinIO / Kafka / OneDrive / …)
     │
     ▼
  SyncLite Consolidator
     │
     ▼
  Destination DB / Data Warehouse / Data Lake
```

## Device Types

| Device Type | Embedded DB | Best For |
|---|---|---|
| `SQLITE` | SQLite | Edge/desktop apps, offline-first, GenAI/RAG |
| `DUCKDB` | DuckDB | Analytical edge workloads, columnar data |
| `DERBY` | Apache Derby | Embedded Java enterprise apps |
| `H2` | H2 | Test environments, Spring Boot apps |
| `HYPERSQL` | HyperSQL | Lightweight in-process SQL apps |
| `SQLITE_APPENDER` | SQLite | High-throughput append-only pipelines |
| `DUCKDB_APPENDER` | DuckDB | Analytical streaming ingestion |
| `DERBY_APPENDER` | Apache Derby | Java streaming ingestion |
| `H2_APPENDER` | H2 | Low-latency append-only writes |
| `HYPERSQL_APPENDER` | HyperSQL | Lightweight append-only streaming |
| `SQLITE_STORE` | SQLite | Key-value CRUD store with SyncLiteStore / Jedis API |
| `DUCKDB_STORE` | DuckDB | Analytical store with SyncLiteStore API |
| `DERBY_STORE` | Apache Derby | Java embedded store with SyncLiteStore API |
| `H2_STORE` | H2 | In-process store with SyncLiteStore API |
| `HYPERSQL_STORE` | HyperSQL | Lightweight store with SyncLiteStore API |
| `STREAMING` | In-memory | Pure log streaming, Kafka Producer / SyncLiteStream API |

## Quick Start

### 1. Add the dependency

```xml
<dependency>
    <groupId>io.synclite</groupId>
    <artifactId>synclite-logger</artifactId>
    <version><!-- latest version --></version>
</dependency>
```

Or copy `synclite-logger-<version>.jar` from the platform release into your project classpath.

### 2. Configure `synclite_logger.conf`

A full sample config file is provided at `logger/src/main/resources/synclite_logger.conf`. At minimum, set:

```properties
# Where to write the local sync logs (staging directory)
local-data-stage-directory=<path/to/stage>

# Where the final destination is (can also be configured in Consolidator UI)
destination-type=SQLITE
```

### 3. Initialize and use in Java

```java
import io.synclite.logger.*;
import java.nio.file.Path;
import java.sql.*;

public class MyEdgeApp {
    public static void main(String[] args) throws Exception {
        Path dbDir = Path.of(System.getProperty("user.home"), "synclite", "db");
        Path dbPath = dbDir.resolve("myapp.db");
        Path conf   = dbDir.resolve("synclite_logger.conf");

        // Initialize SyncLite Logger with SQLite
        Class.forName("io.synclite.logger.SQLite");
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
Class.forName("io.synclite.logger.Streaming");
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
import io.synclite.logger.SQLiteStore;
import io.synclite.logger.SyncLiteStore;

Class.forName("io.synclite.logger.SQLiteStore");
Path dbPath = Path.of("mystore.db");
SQLiteStore.initialize(dbPath, Path.of("synclite_logger.conf"));

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
import io.synclite.logger.Streaming;
import io.synclite.logger.SyncLiteStream;

Class.forName("io.synclite.logger.Streaming");
Path dbPath = Path.of("events.db");
Streaming.initialize(dbPath, Path.of("synclite_logger.conf"));

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

`io.synclite.logger.Jedis` is a drop-in extension of the `redis.clients.jedis.Jedis` class. Every write (strings, hashes, lists, sets, sorted sets, key expiry, delete) is **durably committed to a `SQLITE_STORE` SyncLite device** before being forwarded to Redis. On the next startup the cache is automatically repopulated from the store, so Redis data survives restarts. All captured mutations flow through SyncLite Consolidator to any downstream destination.

```java
import io.synclite.logger.Jedis;

Path dbPath = Path.of("cache.db");

// Managed mode — Jedis handles SQLiteStore initialise / open / close
try (Jedis jedis = Jedis.builder(dbPath, Path.of("synclite_logger.conf"), "jedis-device")
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

The `Jedis.builder(SyncLiteStore)` overload is available when the application manages the store lifecycle externally. See `logger/samples/java/SyncLiteJedisAPIApp.java` for the full sample.

## Python Support

SyncLite Logger also supports Python via JDBC bridge. See `logger/samples/python/` for ready-to-run examples.

## Code Samples

All sample applications live under `logger/samples/`:

```
logger/samples/
├── java/          # Java sample apps (transactional, streaming, appender)
└── python/        # Python sample apps
```

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

```bash
cd synclite-logger-java/logger
mvn -Drevision=oss clean install
```

The compiled JAR is placed under `logger/target/`.

## Related Components

| Component | Role |
|---|---|
| [SyncLite DB](https://github.com/syncliteio/synclite-db) | HTTP server wrapping embedded DBs for any language |
| [SyncLite Client](https://github.com/syncliteio/synclite-client) | CLI to interact with SyncLite devices |
| [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) | Central consolidation server |

## Documentation & Community

- Full documentation: https://www.synclite.io/resources/documentation
- SyncLite Logger configuration reference: https://www.synclite.io/resources/documentation
- Slack workspace: https://join.slack.com/t/syncliteworkspace/shared_invite/zt-2pz945vva-uuKapsubC9Mu~uYDRKo6Jw
- Website: https://www.synclite.io

---

← Back to the [SyncLite Platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)

