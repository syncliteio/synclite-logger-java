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

package io.synclite.logger;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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

    private final Connection conn;
    // Trusted handle — same object as conn, cast once to access the package-private API.
    private final SyncLiteStoreConnection storeConn;
    // Ordered column names per table (lower-cased). LinkedHashSet preserves insertion
    // order so generated INSERT SQL column order is stable.
    private final Map<String, LinkedHashSet<String>> tableColumns = new HashMap<>();
    // PreparedStatement cache.
    // Key for INSERT : table name (lower-cased)
    // Key for UPDATE : "table|U|setCol1,setCol2|whereCol1,whereCol2"
    // Key for DELETE : "table|D|whereCol1,whereCol2"
    // Key for SELECT : "table|S|whereCol1,whereCol2"
    private final Map<String, PreparedStatement> stmtCache = new HashMap<>();
    // table name (lower) → set of cache keys for that table, used for bulk invalidation.
    private final Map<String, Set<String>> tableToKeys = new HashMap<>();
    // SQL type used when a new String-valued column is auto-added via ALTER TABLE.
    // Each backend configures this to the most appropriate unbounded text type.
    private final String stringType;

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
        this.conn = DriverManager.getConnection(urlPrefix + dbPath.toAbsolutePath().toString());
        this.storeConn = (SyncLiteStoreConnection) this.conn;
        this.conn.setAutoCommit(true);
        this.stringType = stringType;
    }

    /**
     * Returns the SQL type that this store uses when automatically adding a new
     * String-valued column via {@code ALTER TABLE}.  The value is backend-specific
     * (e.g. {@code "TEXT"} for SQLite/DuckDB, {@code "VARCHAR(32672)"} for Derby,
     * {@code "LONGVARCHAR"} for HyperSQL).
     */
    public synchronized String getDefaultStringType() {
        return stringType;
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
        if (columnDefs == null || columnDefs.isEmpty()) {
            throw new SQLException("columnDefs must contain at least one column");
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(table).append(" (");
        int i = 0;
        for (Map.Entry<String, String> e : columnDefs.entrySet()) {
            if (i++ > 0) sb.append(", ");
            sb.append(e.getKey()).append(" ").append(e.getValue());
        }
        sb.append(")");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sb.toString());
        } catch (SQLException ex) {
            // Not all databases support IF NOT EXISTS; silently ignore "already exists" errors.
            String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
            String state = ex.getSQLState() == null ? "" : ex.getSQLState();
            if (!state.startsWith("X0Y32") && !msg.contains("already exists") && !msg.contains("already defined")) {
                throw ex;
            }
        }
        invalidateTable(table);
    }

    /**
     * Drops a table if it exists.
     *
     * @param table table name
     */
    public synchronized void dropTable(String table) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE " + table);
        } catch (SQLException ex) {
            // Not all databases support IF EXISTS; silently ignore "table not found" errors.
            String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
            String state = ex.getSQLState() == null ? "" : ex.getSQLState();
            if (!state.startsWith("42Y55") && !msg.contains("does not exist") && !msg.contains("not found") && !msg.contains("unknown table")) {
                throw ex;
            }
        }
        invalidateTable(table);
    }

    // -------------------------------------------------------------------------
    // DML
    // -------------------------------------------------------------------------

    /**
     * Inserts a single row into the table.
     *
     * @param table table name
     * @param row   column name → value pairs. Use a {@link LinkedHashMap} to
     *              guarantee a predictable column order in the generated SQL.
     */
    public synchronized void insert(String table, Map<String, Object> row) throws SQLException {
        if (row == null || row.isEmpty()) {
            throw new SQLException("row must contain at least one column");
        }
        boolean prevAutoCommit = conn.getAutoCommit();
        if (prevAutoCommit) conn.setAutoCommit(false);
        try {
            ensureColumns(table, row);
            Map<String, Object> norm = normalizeKeys(row);
            PreparedStatement pstmt = getInsertStatement(table);
            int pos = 1;
            for (String col : tableColumns.get(table.toLowerCase())) {
                pstmt.setObject(pos++, norm.get(col));
            }
            pstmt.executeUpdate();
            if (prevAutoCommit) conn.commit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { conn.rollback(); } catch (SQLException ignored) {}
                invalidateTable(table);
            }
            throw e;
        } finally {
            if (prevAutoCommit) conn.setAutoCommit(true);
        }
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
        if (rows == null || rows.isEmpty()) return;
        LinkedHashMap<String, Object> representative = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            for (Map.Entry<String, Object> e : row.entrySet()) {
                representative.putIfAbsent(e.getKey(), e.getValue());
            }
        }
        boolean prevAutoCommit = conn.getAutoCommit();
        if (prevAutoCommit) conn.setAutoCommit(false);
        try {
            ensureColumns(table, representative);
            PreparedStatement pstmt = getInsertStatement(table);
            LinkedHashSet<String> orderedCols = tableColumns.get(table.toLowerCase());
            for (Map<String, Object> row : rows) {
                Map<String, Object> norm = normalizeKeys(row);
                int pos = 1;
                for (String col : orderedCols) {
                    pstmt.setObject(pos++, norm.get(col));
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if (prevAutoCommit) conn.commit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { conn.rollback(); } catch (SQLException ignored) {}
                invalidateTable(table);
            }
            throw e;
        } finally {
            if (prevAutoCommit) conn.setAutoCommit(true);
        }
    }

    /**
     * Updates rows in the table matching the {@code where} conditions.
     *
     * @param table table name
     * @param set   column name → new value pairs (at least one required)
     * @param where column name → value pairs for the WHERE clause (AND-combined).
     *              Pass {@code null} or an empty map to update all rows.
     */
    public synchronized void update(String table, Map<String, Object> set, Map<String, Object> where) throws SQLException {
        if (set == null || set.isEmpty()) {
            throw new SQLException("set must contain at least one column");
        }
        boolean prevAutoCommit = conn.getAutoCommit();
        if (prevAutoCommit) conn.setAutoCommit(false);
        try {
            ensureColumns(table, set);
            String cacheKey = updateKey(table, set.keySet(), where == null ? null : where.keySet());
            PreparedStatement pstmt = getOrBuildStatement(cacheKey,
                    () -> buildUpdateSql(table, set.keySet(), where == null ? null : where.keySet()), table);
            int pos = 1;
            for (Object val : set.values()) pstmt.setObject(pos++, val);
            if (where != null) for (Object val : where.values()) pstmt.setObject(pos++, val);
            pstmt.executeUpdate();
            if (prevAutoCommit) conn.commit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { conn.rollback(); } catch (SQLException ignored) {}
                invalidateTable(table);
            }
            throw e;
        } finally {
            if (prevAutoCommit) conn.setAutoCommit(true);
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
        Map<String, Object> firstSet = setList.get(0);
        Map<String, Object> firstWhere = (whereList != null && !whereList.isEmpty()) ? whereList.get(0) : null;
        boolean prevAutoCommit = conn.getAutoCommit();
        if (prevAutoCommit) conn.setAutoCommit(false);
        try {
            ensureColumns(table, firstSet);
            String cacheKey = updateKey(table, firstSet.keySet(), firstWhere == null ? null : firstWhere.keySet());
            PreparedStatement pstmt = getOrBuildStatement(cacheKey,
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
            if (prevAutoCommit) conn.commit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { conn.rollback(); } catch (SQLException ignored) {}
                invalidateTable(table);
            }
            throw e;
        } finally {
            if (prevAutoCommit) conn.setAutoCommit(true);
        }
    }

    /**
     * Deletes rows from the table matching the {@code where} conditions.
     *
     * @param table table name
     * @param where column name → value pairs for the WHERE clause (AND-combined).
     *              Pass {@code null} or an empty map to delete all rows.
     */
    public synchronized void delete(String table, Map<String, Object> where) throws SQLException {
        String cacheKey = deleteKey(table, where == null ? null : where.keySet());
        PreparedStatement pstmt = getOrBuildStatement(cacheKey,
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
        if (whereList == null || whereList.isEmpty()) {
            delete(table, null);
            return;
        }
        Map<String, Object> firstWhere = whereList.get(0);
        String cacheKey = deleteKey(table, firstWhere.isEmpty() ? null : firstWhere.keySet());
        PreparedStatement pstmt = getOrBuildStatement(cacheKey,
                () -> buildDeleteSql(table, firstWhere.isEmpty() ? null : firstWhere.keySet()), table);
        boolean prevAutoCommit = conn.getAutoCommit();
        if (prevAutoCommit) conn.setAutoCommit(false);
        try {
            for (Map<String, Object> whereRow : whereList) {
                int pos = 1;
                for (Object val : whereRow.values()) pstmt.setObject(pos++, val);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if (prevAutoCommit) conn.commit();
        } catch (SQLException e) {
            if (prevAutoCommit) {
                try { conn.rollback(); } catch (SQLException ignored) {}
            }
            throw e;
        } finally {
            if (prevAutoCommit) conn.setAutoCommit(true);
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
        PreparedStatement pstmt = getOrBuildStatement(cacheKey,
                () -> buildSelectSql(table, where == null ? null : where.keySet()), table);
        if (where != null && !where.isEmpty()) {
            int pos = 1;
            for (Object val : where.values()) pstmt.setObject(pos++, val);
        }
        try (ResultSet rs = pstmt.executeQuery()) {
            return toList(rs);
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
        conn.setAutoCommit(autoCommit);
    }

    /** Commits the current transaction. No-op when auto-commit is {@code true}. */
    public synchronized void commit() throws SQLException {
        conn.commit();
    }

    /**
     * Rolls back the current transaction and clears all caches so the next
     * access re-reads the actual schema from the database.
     * No-op when auto-commit is {@code true}.
     */
    public synchronized void rollback() throws SQLException {
        conn.rollback();
        clearAllCaches();
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
        if (!conn.isClosed()) {
            if (!conn.getAutoCommit()) conn.commit();
            for (PreparedStatement ps : stmtCache.values()) {
                try { ps.close(); } catch (SQLException ignored) {}
            }
            stmtCache.clear();
            tableToKeys.clear();
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void clearAllCaches() {
        for (PreparedStatement ps : stmtCache.values()) {
            try { ps.close(); } catch (SQLException ignored) {}
        }
        stmtCache.clear();
        tableColumns.clear();
        tableToKeys.clear();
    }

    /** Functional interface for SQL builders — allows lambda references in getOrBuildStatement. */
    @FunctionalInterface
    private interface SqlBuilder {
        String build() throws SQLException;
    }

    /** Gets or builds a cached PreparedStatement using the given SQL builder. */
    private PreparedStatement getOrBuildStatement(String cacheKey, SqlBuilder sqlBuilder, String table)
            throws SQLException {
        PreparedStatement pstmt = stmtCache.get(cacheKey);
        if (pstmt == null || pstmt.isClosed()) {
            pstmt = storeConn.prepareTrustedStatement(sqlBuilder.build());
            stmtCache.put(cacheKey, pstmt);
            tableToKeys.computeIfAbsent(table.toLowerCase(), k -> new HashSet<>()).add(cacheKey);
        }
        return pstmt;
    }

    private PreparedStatement getInsertStatement(String table) throws SQLException {
        String key = table.toLowerCase();
        PreparedStatement pstmt = stmtCache.get(key);
        if (pstmt == null || pstmt.isClosed()) {
            LinkedHashSet<String> cols = tableColumns.get(key);
            StringBuilder colSb = new StringBuilder();
            StringBuilder phSb = new StringBuilder();
            int i = 0;
            for (String col : cols) {
                if (i++ > 0) { colSb.append(", "); phSb.append(", "); }
                colSb.append(col);
                phSb.append("?");
            }
            pstmt = storeConn.prepareTrustedStatement(
                    "INSERT INTO " + table + " (" + colSb + ") VALUES (" + phSb + ")");
            stmtCache.put(key, pstmt);
            tableToKeys.computeIfAbsent(key, k -> new HashSet<>()).add(key);
        }
        return pstmt;
    }

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

    private void invalidateTable(String table) {
        String key = table.toLowerCase();
        tableColumns.remove(key);
        Set<String> keys = tableToKeys.remove(key);
        if (keys != null) {
            for (String k : keys) {
                PreparedStatement ps = stmtCache.remove(k);
                if (ps != null) { try { ps.close(); } catch (SQLException ignored) {} }
            }
        }
    }

    private Map<String, Object> normalizeKeys(Map<String, Object> row) {
        Map<String, Object> norm = new HashMap<>(row.size());
        for (Map.Entry<String, Object> e : row.entrySet()) {
            norm.put(e.getKey().toLowerCase(), e.getValue());
        }
        return norm;
    }

    private void loadColumns(String table) throws SQLException {
        LinkedHashSet<String> cols = new LinkedHashSet<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + table + " WHERE 1=0");
             ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                cols.add(meta.getColumnLabel(i).toLowerCase());
            }
        }
        tableColumns.put(table.toLowerCase(), cols);
    }

    private String inferSqlType(Object value) {
        if (value instanceof Long || value instanceof Integer ||
                value instanceof Short || value instanceof Byte ||
                value instanceof Boolean) return "INTEGER";
        if (value instanceof Double || value instanceof Float) return "REAL";
        if (value instanceof byte[]) return "BLOB";
        return stringType;
    }

    private void ensureColumns(String table, Map<String, Object> colsWithValues) throws SQLException {
        String key = table.toLowerCase();
        if (!tableColumns.containsKey(key)) loadColumns(table);
        LinkedHashSet<String> known = tableColumns.get(key);
        boolean columnAdded = false;
        for (Map.Entry<String, Object> e : colsWithValues.entrySet()) {
            String col = e.getKey().toLowerCase();
            if (!known.contains(col)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ALTER TABLE " + table + " ADD COLUMN " + col + " " + inferSqlType(e.getValue()));
                }
                known.add(col);
                columnAdded = true;
            }
        }
        if (columnAdded) {
            // Schema changed: evict stale prepared statements for this table.
            Set<String> keys = tableToKeys.remove(key);
            if (keys != null) {
                for (String k : keys) {
                    PreparedStatement ps = stmtCache.remove(k);
                    if (ps != null) { try { ps.close(); } catch (SQLException ignored) {} }
                }
            }
        }
    }

    private List<Map<String, Object>> toList(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= colCount; i++) {
                row.put(meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }
}
