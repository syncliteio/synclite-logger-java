# PR Description

## Summary

Block user SQL from dropping `synclite_txn`, the internal transaction-tracking table present in every SyncLite device database (SQLite, DuckDB, Derby, H2, HyperSQL). Dropping this table via user SQL corrupts the device and makes it unrecoverable.

---

## Background

Every SyncLite device database file maintains a table named `synclite_txn` that SyncLite uses internally to track transaction state. This table must not be dropped by user-issued SQL. Previously there was no guard preventing this.

---

## What Changed

Added `SyncLiteUtils.validateProtectedInternalTableDDL(String sql, String tableName)` which throws `SQLException` if a `DROP TABLE` targets `synclite_txn` (case-insensitive match).

The guard is called from all user-facing SQL execution paths:

| Class | Device scope |
|---|---|
| `DBLoggerStatement` | DBLogger, Streaming (Statement) |
| `DBLoggerPreparedStatement` | DBLogger, Streaming (PreparedStatement) |
| `SyncLiteStoreStatement` | Store, Appender (Statement) |
| `SyncLiteStorePreparedStatement` | Store, Appender (PreparedStatement) |
| `SyncLiteStatement` | SQLite, DuckDB, Derby, H2, HyperSQL transactional (Statement) |
| `SyncLitePreparedStatement` | SQLite, DuckDB, Derby, H2, HyperSQL transactional (PreparedStatement) |

For prepared statements the guard fires at `Connection.prepareStatement()` construction time, before any `execute()` call. Internal SyncLite cleanup paths that use raw JDBC (e.g. `SQLLogger.resetDeviceCheckpoint()`) bypass all wrappers and are not affected.

---

## Files Changed

**Main:**
- `logger/src/main/java/io/synclite/logger/SyncLiteUtils.java`
- `logger/src/main/java/io/synclite/logger/DBLoggerStatement.java`
- `logger/src/main/java/io/synclite/logger/DBLoggerPreparedStatement.java`
- `logger/src/main/java/io/synclite/logger/SyncLiteStoreStatement.java`
- `logger/src/main/java/io/synclite/logger/SyncLiteStorePreparedStatement.java`
- `logger/src/main/java/io/synclite/logger/SyncLiteStatement.java`
- `logger/src/main/java/io/synclite/logger/SyncLitePreparedStatement.java`

**Tests:**
- `logger/src/test/java/io/synclite/logger/StreamingTest.java`
- `logger/src/test/java/io/synclite/logger/SQLiteStoreTest.java`

---

## Testing

- `StreamingTest.testBasicTableOperations` — asserts `DROP TABLE synclite_txn` throws via both `Statement.execute()` and `Connection.prepareStatement()`, and confirms the table remains queryable afterwards.
- `SQLiteStoreTest.testBasicTableOperations` — same assertions for the Store device path.
- All 4 targeted test cases pass.

---

## Risk Assessment

Low. The change only adds a reject path for one specific DDL pattern (`DROP TABLE synclite_txn`). All other DDL on user tables is unaffected. Internal SyncLite reset/cleanup code is not routed through the guarded paths.

---

## Suggested Reviewer Checks

- Confirm `DROP TABLE synclite_txn` is blocked for all device types (SQLite, DuckDB, Derby, H2, HyperSQL).
- Confirm `CREATE TABLE`, `ALTER TABLE`, and `DROP TABLE` on user tables are unaffected.
- Confirm internal device reset/cleanup is not broken.
