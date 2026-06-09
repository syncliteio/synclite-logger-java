/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package io.synclite;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Agent-friendly high-level API for SyncLite Store devices.
 *
 * <p>Provides simple insert/update/delete/select operations on tables inside a
 * store device without requiring the caller to write any SQL. The underlying
 * device captures every mutation to the SyncLite replication log so all
 * changes are available for downstream CDC consumption.
 *
 * <p>Obtain an instance via a store-specific factory, e.g.:
 * <pre>
 *   SQLiteStore.initialize(dbPath);
 *   try (SyncLiteStore store = SQLiteStore.open(dbPath)) {
 *       store.createTable("memories", Map.of("key", "TEXT PRIMARY KEY", "value", "TEXT"));
 *       store.insert("memories", Map.of("key", "ctx1", "value", "agent remembered this"));
 *       store.update("memories", Map.of("value", "updated memory"), Map.of("key", "ctx1"));
 *       List&lt;Map&lt;String, Object&gt;&gt; rows = store.selectAll("memories");
 *       store.delete("memories", Map.of("key", "ctx1"));
 *   }
 * </pre>
 *
 * <p>The default auto-commit mode is {@code true}: each write operation is
 * committed immediately. Call {@link #setAutoCommit(boolean) setAutoCommit(false)}
 * to batch multiple operations into a single transaction and commit explicitly
 * with {@link #commit()}.
 *
 * <p><strong>Thread safety:</strong> All public methods are {@code synchronized},
 * so a single {@code SyncLiteStore} instance can safely be shared across threads
 * for auto-commit workloads. For maximum write throughput on backends that support
 * concurrent connections (H2, Derby, DuckDB, HyperSQL), open one instance per
 * thread — each gets its own connection and the two stores operate fully in
 * parallel. For transactional use ({@code setAutoCommit(false)}), do not share an
 * instance across threads; use one instance per thread.
 */
public class SyncLiteStore implements AutoCloseable {

    // All shared write mechanics (connection, caches, insert, batch, txn) live here.
    private final SyncLiteTableWriter writer;

    /**
     * Package-private: callers obtain instances via {@code <BackendStore>.open()}.
     */
    SyncLiteStore(Path dbPath, String urlPrefix) throws SQLException {
        this(dbPath, urlPrefix, "VARCHAR(255)");
    }

    /**
     * Package-private: callers obtain instances via {@code <BackendStore>.open()}.
     *
     * @param stringType the SQL column type to use when auto-adding a String-valued
     *                   column (e.g. {@code "TEXT"}, {@code "VARCHAR(32672)"},
     *                   {@code "LONGVARCHAR"}).  Call
     *                   {@link #getDefaultStringType()} on the returned instance to
     *                   inspect the value.
     */
    SyncLiteStore(Path dbPath, String urlPrefix, String stringType) throws SQLException {
        this(dbPath, urlPrefix, stringType, false);
    }

    /** Package-private: used by {@link SQLiteStore#openUnlogged} and other backends. */
    SyncLiteStore(Path dbPath, String urlPrefix, String stringType, boolean allUnlogged) throws SQLException {
        this.writer = new SyncLiteTableWriter(dbPath, urlPrefix, stringType, allUnlogged);
    }

    /**
     * Returns the SQL type that this store uses when automatically adding a new
     * String-valued column via {@code ALTER TABLE}.  The value is backend-specific
     * (e.g. {@code "TEXT"} for SQLite/DuckDB, {@code "VARCHAR(32672)"} for Derby,
     * {@code "LONGVARCHAR"} for HyperSQL).
     */
    public synchronized String getDefaultStringType() {
        return writer.getDefaultStringType();
    }

    // -------------------------------------------------------------------------
    // Schema
    // -------------------------------------------------------------------------

    /**
     * Creates a table if it does not already exist.
     *
     * @param table      table name
     * @param columnDefs ordered map of column name → SQL type (e.g. {@code "TEXT"}, {@code "INTEGER"}).
     *                   Use a {@link LinkedHashMap} to guarantee column order.
     */
    public synchronized void createTable(String table, Map<String, String> columnDefs) throws SQLException {
        writer.createTable(table, columnDefs);
    }

    /**
     * Drops a table if it exists.
     *
     * @param table table name
     */
    public synchronized void dropTable(String table) throws SQLException {
        writer.dropTable(table);
    }

    /**
     * Renames a table.
     *
     * @param oldTable current table name
     * @param newTable new table name
     */
    public synchronized void renameTable(String oldTable, String newTable) throws SQLException {
        writer.conn.createStatement().execute("ALTER TABLE " + oldTable + " RENAME TO " + newTable);
    }

    // -------------------------------------------------------------------------
    // DML — insert
    // -------------------------------------------------------------------------

    /**
     * Inserts a single row into the table.
     *
     * @param table table name
     * @param row   column name → value pairs. Use a {@link LinkedHashMap} to
     *              guarantee a predictable column order in the generated SQL.
     */
    public synchronized void insert(String table, Map<String, Object> row) throws SQLException {
        writer.insert(table, row); // routing to unlogged is handled inside writer.insert()
    }

    /**
     * Inserts multiple rows into the table in a single batched operation.
     *
     * <p>If rows have different column sets, the union of all column names is used
     * and missing values are set to {@code null}.
     *
     * @param table table name
     * @param rows  list of rows; each row is a column name → value map
     */
    public synchronized void insertBatch(String table, List<Map<String, Object>> rows) throws SQLException {
        writer.insertBatch(table, rows); // routing to unlogged is handled inside writer.insertBatch()
    }

    // -------------------------------------------------------------------------
    // DML — update
    // -------------------------------------------------------------------------

    /**
     * Updates rows in the table matching the {@code where} conditions.
     *
     * @param table table name
     * @param set   column name → new value pairs (at least one required)
     * @param where column name → value pairs for the WHERE clause (AND-combined).
     *              Pass {@code null} or an empty map to update all rows.
     */
    public synchronized void update(String table, Map<String, Object> set, Map<String, Object> where) throws SQLException {
        if (writer.isUnloggedFor(table)) { updateUnlogged(table, set, where); return; }
        if (set == null || set.isEmpty()) {
            throw new SQLException("set must contain at least one column");
        }
        boolean prevAutoCommit = writer.conn.getAutoCommit();
        if (prevAutoCommit) writer.conn.setAutoCommit(false);
        try {
            writer.ensureColumns(table, set);
            String cacheKey = updateKey(table, set.keySet(), where == null ? null : where.keySet());
            PreparedStatement pstmt = writer.getOrBuildStatement(cacheKey,
                    () -> buildUpdateSql(table, set.keySet(), where == null ? null : where.keySet()), table);
            int pos = 1;
            for (Object val : set.values()) pstmt.setObject(pos++, val);
            if (where != null) for (Object val : where.values()) pstmt.setObject(pos++, val);
            pstmt.executeUpdate();
            if (prevAutoCommit) writer.conn.commit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { writer.conn.rollback(); } catch (SQLException ignored) {}
                writer.invalidateTable(table);
            }
            throw e;
        } finally {
            if (prevAutoCommit) writer.conn.setAutoCommit(true);
        }
    }

    /**
     * Updates multiple rows in a single batched operation.
     *
     * <p>All entries in {@code setList} and {@code whereList} must have the same
     * column keys — the shape of the first entry determines the prepared SQL.
     *
     * @param table     table name
     * @param setList   list of SET column → value maps (one per row)
     * @param whereList list of WHERE column → value maps (one per row, same size as setList)
     */
    public synchronized void updateBatch(String table, List<Map<String, Object>> setList,
            List<Map<String, Object>> whereList) throws SQLException {
        if (setList == null || setList.isEmpty()) return;
        if (writer.isUnloggedFor(table)) { updateBatchUnlogged(table, setList, whereList); return; }
        Map<String, Object> firstSet = setList.get(0);
        Map<String, Object> firstWhere = (whereList != null && !whereList.isEmpty()) ? whereList.get(0) : null;
        boolean prevAutoCommit = writer.conn.getAutoCommit();
        if (prevAutoCommit) writer.conn.setAutoCommit(false);
        try {
            writer.ensureColumns(table, firstSet);
            String cacheKey = updateKey(table, firstSet.keySet(), firstWhere == null ? null : firstWhere.keySet());
            PreparedStatement pstmt = writer.getOrBuildStatement(cacheKey,
                    () -> buildUpdateSql(table, firstSet.keySet(), firstWhere == null ? null : firstWhere.keySet()), table);
            for (int idx = 0; idx < setList.size(); idx++) {
                Map<String, Object> setRow = setList.get(idx);
                Map<String, Object> whereRow = (whereList != null && idx < whereList.size()) ? whereList.get(idx) : null;
                int pos = 1;
                for (Object val : setRow.values()) pstmt.setObject(pos++, val);
                if (whereRow != null) for (Object val : whereRow.values()) pstmt.setObject(pos++, val);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if (prevAutoCommit) writer.conn.commit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { writer.conn.rollback(); } catch (SQLException ignored) {}
                writer.invalidateTable(table);
            }
            throw e;
        } finally {
            if (prevAutoCommit) writer.conn.setAutoCommit(true);
        }
    }

    // -------------------------------------------------------------------------
    // DML — delete
    // -------------------------------------------------------------------------

    /**
     * Deletes rows from the table matching the {@code where} conditions.
     *
     * @param table table name
     * @param where column name → value pairs for the WHERE clause (AND-combined).
     *              Pass {@code null} or an empty map to delete all rows.
     */
    public synchronized void delete(String table, Map<String, Object> where) throws SQLException {
        if (writer.isUnloggedFor(table)) { deleteUnlogged(table, where); return; }
        String cacheKey = deleteKey(table, where == null ? null : where.keySet());
        PreparedStatement pstmt = writer.getOrBuildStatement(cacheKey,
                () -> buildDeleteSql(table, where == null ? null : where.keySet()), table);
        if (where != null) {
            int pos = 1;
            for (Object val : where.values()) pstmt.setObject(pos++, val);
        }
        pstmt.executeUpdate();
    }

    /**
     * Deletes multiple rows in a single batched operation.
     *
     * <p>All entries in {@code whereList} must have the same column keys.
     *
     * @param table     table name
     * @param whereList list of WHERE column → value maps; pass {@code null} or empty to delete all rows
     */
    public synchronized void deleteBatch(String table, List<Map<String, Object>> whereList) throws SQLException {
        if (whereList == null || whereList.isEmpty()) { delete(table, null); return; }
        if (writer.isUnloggedFor(table)) { deleteBatchUnlogged(table, whereList); return; }
        Map<String, Object> firstWhere = whereList.get(0);
        String cacheKey = deleteKey(table, firstWhere.isEmpty() ? null : firstWhere.keySet());
        PreparedStatement pstmt = writer.getOrBuildStatement(cacheKey,
                () -> buildDeleteSql(table, firstWhere.isEmpty() ? null : firstWhere.keySet()), table);
        boolean prevAutoCommit = writer.conn.getAutoCommit();
        if (prevAutoCommit) writer.conn.setAutoCommit(false);
        try {
            for (Map<String, Object> whereRow : whereList) {
                int pos = 1;
                for (Object val : whereRow.values()) pstmt.setObject(pos++, val);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if (prevAutoCommit) writer.conn.commit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { writer.conn.rollback(); } catch (SQLException ignored) {}
            }
            throw e;
        } finally {
            if (prevAutoCommit) writer.conn.setAutoCommit(true);
        }
    }

    // -------------------------------------------------------------------------
    // Query
    // -------------------------------------------------------------------------

    /**
     * Returns all rows from the table.
     *
     * @param table table name
     * @return list of rows; each row is a {@link LinkedHashMap} of column name → value
     */
    public synchronized List<Map<String, Object>> selectAll(String table) throws SQLException {
        return select(table, null);
    }

    /**
     * Returns rows from the table matching the {@code where} conditions.
     *
     * @param table table name
     * @param where column name → value pairs (AND-combined). Pass {@code null}
     *              or an empty map to return all rows.
     * @return list of rows; each row is a {@link LinkedHashMap} of column name → value
     */
    public synchronized List<Map<String, Object>> select(String table, Map<String, Object> where) throws SQLException {
        String cacheKey = selectKey(table, where == null ? null : where.keySet());
        PreparedStatement pstmt = writer.getOrBuildStatement(cacheKey,
                () -> buildSelectSql(table, where == null ? null : where.keySet()), table);
        if (where != null && !where.isEmpty()) {
            int pos = 1;
            for (Object val : where.values()) pstmt.setObject(pos++, val);
        }
        try (ResultSet rs = pstmt.executeQuery()) {
            return writer.toList(rs);
        }
    }

    // -------------------------------------------------------------------------
    // Unlogged DML — write to device DB without CDC log entries
    // -------------------------------------------------------------------------

    /**
     * Disables or re-enables CDC logging for {@code table} on this instance.
     *
     * <p>When logging is disabled ({@code logged = false}), any subsequent
     * {@link #insert}, {@link #insertBatch}, {@link #update}, {@link #updateBatch},
     * {@link #delete}, and {@link #deleteBatch} calls targeting that table will
     * write data to the device DB but will <em>not</em> generate commandlog entries.
     * Downstream SyncLite consumers therefore do not see those writes.
     *
     * @param table  table name
     * @param logged {@code false} to suppress CDC logging; {@code true} (default) to restore it
     */
    public synchronized void setTableLogging(String table, boolean logged) {
        writer.setTableLogging(table, logged);
    }

    /** Inserts a row without generating a CDC log entry. */
    public synchronized void insertUnlogged(String table, Map<String, Object> row) throws SQLException {
        writer.insertUnlogged(table, row);
    }

    /** Inserts multiple rows without generating CDC log entries. */
    public synchronized void insertBatchUnlogged(String table, List<Map<String, Object>> rows) throws SQLException {
        writer.insertBatchUnlogged(table, rows);
    }

    /** Updates rows without generating a CDC log entry. */
    public synchronized void updateUnlogged(String table, Map<String, Object> set, Map<String, Object> where)
            throws SQLException {
        if (set == null || set.isEmpty()) throw new SQLException("set must contain at least one column");
        boolean prevAutoCommit = writer.conn.getAutoCommit();
        if (prevAutoCommit) writer.conn.setAutoCommit(false);
        try {
            writer.ensureColumns(table, set);
            String cacheKey = updateKey(table, set.keySet(), where == null ? null : where.keySet());
            PreparedStatement pstmt = writer.getUnloggedOrBuildStatement(cacheKey,
                    () -> buildUpdateSql(table, set.keySet(), where == null ? null : where.keySet()), table);
            int pos = 1;
            for (Object val : set.values()) pstmt.setObject(pos++, val);
            if (where != null) for (Object val : where.values()) pstmt.setObject(pos++, val);
            pstmt.executeUpdate();
            if (prevAutoCommit) writer.nativeCommit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { writer.conn.rollback(); } catch (SQLException ignored) {}
                writer.invalidateTable(table);
            }
            throw e;
        } finally {
            if (prevAutoCommit) writer.conn.setAutoCommit(true);
        }
    }

    /** Updates multiple rows without generating CDC log entries. */
    public synchronized void updateBatchUnlogged(String table, List<Map<String, Object>> setList,
            List<Map<String, Object>> whereList) throws SQLException {
        if (setList == null || setList.isEmpty()) return;
        Map<String, Object> firstSet   = setList.get(0);
        Map<String, Object> firstWhere = (whereList != null && !whereList.isEmpty()) ? whereList.get(0) : null;
        boolean prevAutoCommit = writer.conn.getAutoCommit();
        if (prevAutoCommit) writer.conn.setAutoCommit(false);
        try {
            writer.ensureColumns(table, firstSet);
            String cacheKey = updateKey(table, firstSet.keySet(), firstWhere == null ? null : firstWhere.keySet());
            PreparedStatement pstmt = writer.getUnloggedOrBuildStatement(cacheKey,
                    () -> buildUpdateSql(table, firstSet.keySet(), firstWhere == null ? null : firstWhere.keySet()), table);
            for (int idx = 0; idx < setList.size(); idx++) {
                Map<String, Object> setRow   = setList.get(idx);
                Map<String, Object> whereRow = (whereList != null && idx < whereList.size()) ? whereList.get(idx) : null;
                int pos = 1;
                for (Object val : setRow.values()) pstmt.setObject(pos++, val);
                if (whereRow != null) for (Object val : whereRow.values()) pstmt.setObject(pos++, val);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if (prevAutoCommit) writer.nativeCommit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { writer.conn.rollback(); } catch (SQLException ignored) {}
                writer.invalidateTable(table);
            }
            throw e;
        } finally {
            if (prevAutoCommit) writer.conn.setAutoCommit(true);
        }
    }

    /** Deletes rows without generating a CDC log entry. */
    public synchronized void deleteUnlogged(String table, Map<String, Object> where) throws SQLException {
        String cacheKey = deleteKey(table, where == null ? null : where.keySet());
        PreparedStatement pstmt = writer.getUnloggedOrBuildStatement(cacheKey,
                () -> buildDeleteSql(table, where == null ? null : where.keySet()), table);
        boolean prevAutoCommit = writer.conn.getAutoCommit();
        if (prevAutoCommit) writer.conn.setAutoCommit(false);
        try {
            if (where != null) {
                int pos = 1;
                for (Object val : where.values()) pstmt.setObject(pos++, val);
            }
            pstmt.executeUpdate();
            if (prevAutoCommit) writer.nativeCommit();
        } catch (SQLException e) {
            if (prevAutoCommit) try { writer.conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        } finally {
            if (prevAutoCommit) writer.conn.setAutoCommit(true);
        }
    }

    /** Deletes multiple rows without generating CDC log entries. */
    public synchronized void deleteBatchUnlogged(String table, List<Map<String, Object>> whereList)
            throws SQLException {
        if (whereList == null || whereList.isEmpty()) { deleteUnlogged(table, null); return; }
        Map<String, Object> firstWhere = whereList.get(0);
        String cacheKey = deleteKey(table, firstWhere.isEmpty() ? null : firstWhere.keySet());
        PreparedStatement pstmt = writer.getUnloggedOrBuildStatement(cacheKey,
                () -> buildDeleteSql(table, firstWhere.isEmpty() ? null : firstWhere.keySet()), table);
        boolean prevAutoCommit = writer.conn.getAutoCommit();
        if (prevAutoCommit) writer.conn.setAutoCommit(false);
        try {
            for (Map<String, Object> whereRow : whereList) {
                int pos = 1;
                for (Object val : whereRow.values()) pstmt.setObject(pos++, val);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if (prevAutoCommit) writer.nativeCommit();
        } catch (SQLException e) {
            if (prevAutoCommit) try { writer.conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        } finally {
            if (prevAutoCommit) writer.conn.setAutoCommit(true);
        }
    }

    // -------------------------------------------------------------------------
    // Transaction control
    // -------------------------------------------------------------------------

    /**
     * Sets auto-commit mode. When {@code false}, subsequent write operations
     * are not committed until {@link #commit()} is called explicitly.
     *
     * <p>For transactional use, do not share this instance across threads —
     * create one {@code SyncLiteStore} per thread instead.
     */
    public synchronized void setAutoCommit(boolean autoCommit) throws SQLException {
        writer.setAutoCommit(autoCommit);
    }

    /** Commits the current transaction. No-op when auto-commit is {@code true}. */
    public synchronized void commit() throws SQLException {
        writer.commit();
    }

    /**
     * Rolls back the current transaction and clears all caches so the next
     * access re-reads the actual schema from the database.
     * No-op when auto-commit is {@code true}.
     */
    public synchronized void rollback() throws SQLException {
        writer.rollback();
    }

    // -------------------------------------------------------------------------
    // AutoCloseable
    // -------------------------------------------------------------------------

    /**
     * Commits any pending transaction and closes the underlying connection.
     * Safe to call inside a try-with-resources block.
     */
    @Override
    public synchronized void close() throws SQLException {
        writer.close();
    }

    // -------------------------------------------------------------------------
    // Internal SQL builders (store-only: update / delete / select)
    // -------------------------------------------------------------------------

    private String updateKey(String table, Set<String> setKeys, Set<String> whereKeys) {
        return table.toLowerCase() + "|U|" + String.join(",", setKeys)
                + "|" + (whereKeys == null ? "" : String.join(",", whereKeys));
    }

    private String deleteKey(String table, Set<String> whereKeys) {
        return table.toLowerCase() + "|D|" + (whereKeys == null ? "" : String.join(",", whereKeys));
    }

    private String selectKey(String table, Set<String> whereKeys) {
        return table.toLowerCase() + "|S|" + (whereKeys == null ? "" : String.join(",", whereKeys));
    }

    private String buildUpdateSql(String table, Set<String> setKeys, Set<String> whereKeys) {
        StringBuilder sb = new StringBuilder("UPDATE ").append(table).append(" SET ");
        int i = 0;
        for (String col : setKeys) {
            if (i++ > 0) sb.append(", ");
            sb.append(col).append(" = ?");
        }
        if (whereKeys != null && !whereKeys.isEmpty()) {
            sb.append(" WHERE ");
            int j = 0;
            for (String col : whereKeys) {
                if (j++ > 0) sb.append(" AND ");
                sb.append(col).append(" = ?");
            }
        }
        return sb.toString();
    }

    private String buildDeleteSql(String table, Set<String> whereKeys) {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(table);
        if (whereKeys != null && !whereKeys.isEmpty()) {
            sb.append(" WHERE ");
            int j = 0;
            for (String col : whereKeys) {
                if (j++ > 0) sb.append(" AND ");
                sb.append(col).append(" = ?");
            }
        }
        return sb.toString();
    }

    private String buildSelectSql(String table, Set<String> whereKeys) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(table);
        if (whereKeys != null && !whereKeys.isEmpty()) {
            sb.append(" WHERE ");
            int j = 0;
            for (String col : whereKeys) {
                if (j++ > 0) sb.append(" AND ");
                sb.append(col).append(" = ?");
            }
        }
        return sb.toString();
    }
}
