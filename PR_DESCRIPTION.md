# PR Description — synclite-logger-java

## Summary

This PR delivers security hardening, data-durability improvements, resource-leak fixes, correctness bug fixes for archiver components, and expanded documentation covering the `SyncLiteStore`, `SyncLiteStream`, and Jedis (Redis-compatible) APIs.

---

## Changes by File

### `logger/pom.xml` — Dependency version updates

| Dependency | Before | After |
|---|---|---|
| `org.xerial:sqlite-jdbc` | 3.43.0.0 | **3.53.0.0** |
| `org.duckdb:duckdb_jdbc` | 1.3.2.1 | **1.5.2.0** |

Both updates align this module with all other SyncLite submodules and pick up upstream bug fixes and CVE patches in both drivers.

---

### `src/.../FSArchiver.java` — Encryption algorithm upgrade and key caching

**Security fix (OWASP A02:2021 — Cryptographic Failures):**

The RSA encryption algorithm used to protect sync packages was `RSA/ECB/PKCS1Padding`. This padding scheme is known to be vulnerable to several adaptive chosen-ciphertext attacks (Bleichenbacher's attack and variants). It has been replaced with `RSA/ECB/OAEPWithSHA-256AndMGF1Padding` using explicit `OAEPParameterSpec`.

Changes:
- Cipher algorithm string: `"RSA/ECB/PKCS1Padding"` → `"RSA/ECB/OAEPWithSHA-256AndMGF1Padding"`
- New `OAEPParameterSpec` constructed with `SHA-256` hash, `MGF1` mask generation function, and `MGF1ParameterSpec.SHA256`
- New `cachedPublicKey` instance field: the public key file was previously read and parsed on every encryption call. It is now read once at first use and cached in the field. This avoids repeated file I/O and `X509EncodedKeySpec` parsing per chunk.
- New imports: `OAEPParameterSpec`, `MGF1ParameterSpec`, `InvalidAlgorithmParameterException`

---

### `src/.../KafkaArchiver.java` — Resource leak fix and chunk-calculation bug fix

**Resource fix:**

`AdminClient` was instantiated and used to check topic existence, but the `close()` call was only reached via the `finally` block of the outer method in the non-exception path. On the exception path (e.g., Kafka unavailable), the `AdminClient` was never closed. Wrapped in `try-with-resources` to guarantee closure.

**Bug fix — file chunking for exact multiples of `FILE_CHUNK_SIZE`:**

When `fileSize % FILE_CHUNK_SIZE == 0` (file size is an exact multiple of the chunk size), the old code computed:

```java
int lastChunkSize = (int)(fileSize % FILE_CHUNK_SIZE); // = 0
int numChunks = (int)(fileSize / FILE_CHUNK_SIZE);     // under-counted by 1
```

This produced a `lastChunkSize` of 0 and missed the final chunk, causing incomplete file transfer for those files. Fixed to:

```java
if (fileSize % FILE_CHUNK_SIZE == 0) {
    numChunks = (int)(fileSize / FILE_CHUNK_SIZE);
    lastChunkSize = FILE_CHUNK_SIZE;
} else {
    numChunks = (int)(fileSize / FILE_CHUNK_SIZE) + 1;
    lastChunkSize = (int)(fileSize % FILE_CHUNK_SIZE);
}
```

---

### `src/.../SFTPArchiver.java` — Null-check logic correction

In `uploadFile()`, the guard condition was:

```java
if (this.remoteCommandDirectory != null) { ... }
```

`this.remoteCommandDirectory` is the class field, which is initialized at object construction. However, the parameter passed to the method was the local variable `remoteCommandDirectory` (without `this`). The check against `this.remoteCommandDirectory` was always operating on the already-initialized field rather than evaluating the method argument. Fixed to:

```java
if (remoteCommandDirectory != null) { ... }
```

---

### `src/.../SQLLogger.java` — SQLite pragma changes and `reloadCurrentLogSegment` refactor

**SQLite pragma hardening:**

SyncLite log segments are durability-critical. The previous pragmas traded safety for performance in ways inappropriate for a write-ahead log used for data replication:

| Pragma | Before | After | Reason |
|---|---|---|---|
| `journal_mode` | `normal` | `delete` | `normal` mode means WAL is never checkpointed automatically; `delete` mode keeps the classic rollback journal and guarantees durability per-commit |
| `synchronous` | `normal` | `full` | `normal` can lose the last committed transaction on OS crash; `full` guarantees fsync after every commit |
| `locking_mode` | _(not set)_ | `exclusive` | Prevents other processes from reading partial writes during active logging |
| `mmap_size` | `30000000000` (30 GB) | `0` | 30 GB mmap is inappropriate for edge devices with limited virtual address space; disabled |

**`reloadCurrentLogSegment()` refactor:**

The method was opening a new JDBC connection to the log file to reload segment metadata, even though an active connection already existed from `initLogSegment()`. This created a second concurrent connection incompatible with `locking_mode=exclusive` and was unnecessary. The method now resets local state variables and reuses the existing connection.

---

### `src/.../SQLStager.java` — SQLite pragma changes (same as SQLLogger)

Same `journal_mode`, `synchronous`, `locking_mode`, and `mmap_size` changes as `SQLLogger.java` applied to the stager database that caches log segments awaiting upload to the configured stage storage.

---

### `src/.../SyncLiteFileSerializer.java` — IOException now properly propagated

In `serialize()`, an `IOException` thrown during binary serialization was previously silently swallowed:

```java
} catch (IOException e) {
    // will be handled in caller by checking bytes
}
```

However, the caller checked only whether the returned byte array was non-null. If serialization failed halfway, the caller could not distinguish a partial write from an empty result. The `IOException` is now caught and re-thrown as `SerializationException`, ensuring the archiver and upload pipeline are informed of the failure and can retry or abort rather than silently uploading a corrupt payload.

---

### `src/.../SyncTxnLogger.java` — Rollback not flushing log segment

In `logRollbackAndFlush()`, the method was clearing the in-memory transaction state (releasing the transaction lock) before calling `commitLogSegment()`. This meant the rollback record could remain buffered in the WAL and never be written to the durable log segment if the process crashed between the state clear and the segment commit.

Fixed ordering: `commitLogSegment()` is now called **before** clearing the transaction state, ensuring the rollback record is durably persisted before the transaction lock is released.

---

### `README.md` — Expanded with STORE device types and new API sections

- **Device Types table:** Added five new rows for `STORE` device types (`SQLITE_STORE`, `DUCKDB_STORE`, `DERBY_STORE`, `H2_STORE`, `HYPERSQL_STORE`). The `STREAMING` row was updated to mention the `SyncLiteStream` API alongside the Appender API. The primary use-case column for each was clarified.
- **Section 5 — SyncLiteStore API:** New section. Describes the `SyncLiteStore` interface that provides type-safe CRUD operations (insert, upsert, update, delete, query) over a SQLite/DuckDB/Derby/H2/HyperSQL device without writing raw SQL. Includes a complete Java code sample demonstrating table creation, insert, upsert, update, delete, and `closeAllDevices()` call.
- **Section 6 — SyncLiteStream API:** New section. Describes the fluent append-only ingestion API over a STREAMING device. Includes a Java code sample showing `SyncLiteStream.builder()` chain, `stream.publishRecord()`, and `closeAllDevices()`.
- **Section 7 — Jedis (Redis-Compatible) API:** New section. Describes using the Jedis client library against a SyncLite STREAMING device that emulates a Redis server. Includes a Java code sample covering strings (`set`/`get`/`expire`/`del`), hashes (`hset`/`hget`/`hmset`/`hgetAll`), lists (`lpush`/`lrange`), sets (`sadd`/`smembers`), and sorted sets (`zadd`/`zrangeWithScores`). Both builder modes (managed lifecycle, plain constructor) illustrated.

- Practical differentiator called out in docs: `INSERT INTO ... SELECT ...` is naturally a SQL-device pattern, not the core Store-device model.

---

## Why

The previous sample set and docs were inconsistent with the desired canonical structure and did not clearly communicate the SQL-device vs Store-device replication model.

This PR makes the samples easier to consume, better aligned across Java/Python bridges, and clearer about when to use each device/API surface.

---

## What Changed

### 1) Canonical sample set and layout

- Added/organized canonical Java samples under `logger/samples/java`.
- Split Python samples into bridge-specific folders under `logger/samples/python`:
  - `JayDeBeApi` for SQL/JDBC-oriented Python usage
  - `JPype` for direct Java API usage from Python
- Added sample index/readme files:
  - `logger/samples/README.md`
  - `logger/samples/java/README.md`
  - `logger/samples/python/README.md`
  - `logger/samples/python/JayDeBeApi/README.md`
  - `logger/samples/python/JPype/README.md`
- Updated root `README.md` to point users to local samples.

### 2) Sample behavior parity and clarity

Expanded relevant samples to demonstrate fuller end-to-end operations:

- SQL/Store-oriented samples now demonstrate create/insert/update/delete/schema evolution/drop-table patterns where applicable.
- Streaming samples explicitly demonstrate append + schema evolution flows and expected UPDATE/DELETE failures.
- Java and Python samples were aligned for equivalent intent where each bridge/API supports it.
- Added a JPype Jedis API sample with broader command coverage (strings/hashes/lists/sets/zsets/TTL lifecycle) to match the richer Java Jedis sample intent.

### 3) Jedis managed lifecycle support

Enhanced `Jedis` with managed-store builder overloads:

- `Jedis.builder(Path storeDbPath, Path configPath)`
- `Jedis.builder(Path storeDbPath, Path configPath, String deviceName)`

Behavior:

- In managed mode, `build()` performs SQLiteStore initialize/open internally.
- `Jedis.close()` closes the managed store/device lifecycle.
- Existing explicit-store mode (`builder(SyncLiteStore)`) remains available.

### 4) Jedis tests adapted

- Added `testManagedBuilderAutoInitializesStoreLifecycle()` in `logger/src/test/java/io/synclite/logger/JedisTest.java`.
- Test validates:
  - Managed builder can initialize/open and persist values.
  - Data durability in underlying store.
  - Redis warm-up restoration path using managed builder only.

### 5) Documentation style refresh in samples

- Replaced short label-style doc headers with descriptive, scenario-oriented explanations.
- Clarified SQL-device vs Store-device semantics in both Java and Python sample docs.

---

## Key Files

### Core code

- `logger/src/main/java/io/synclite/logger/Jedis.java`

### Tests

- `logger/src/test/java/io/synclite/logger/JedisTest.java`

### Sample indexes/docs

- `README.md`
- `logger/samples/README.md`
- `logger/samples/java/README.md`
- `logger/samples/python/README.md`
- `logger/samples/python/JayDeBeApi/README.md`
- `logger/samples/python/JPype/README.md`

### Sample apps

- Java canonical sample set in `logger/samples/java/`
- Python JayDeBeApi sample set in `logger/samples/python/JayDeBeApi/`
- Python JPype sample set in `logger/samples/python/JPype/`

---

## Validation

Completed:

- Static validation after sample/doc updates reported no editor errors in:
  - `logger/samples/java`
  - `logger/samples/python`
- Confirmed old label-style markers were removed from sample doc headers.

Not fully completed in this branch session:

- Full Java test run was attempted earlier but cancelled before completion.
- Python sample runtime smoke tests were not executed end-to-end in this session.

---

## Backward Compatibility / Risk

- Low-to-medium risk for runtime behavior:
  - `Jedis` gains new managed lifecycle path but keeps existing explicit-store builder usage.
  - Added close-time lifecycle handling only for managed mode.
- Low risk for sample/docs changes:
  - Most changes are sample additions, organization, and explanatory documentation.

---