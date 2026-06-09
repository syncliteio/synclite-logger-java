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
 * Package-private shared core for {@link SyncLiteStore} and {@link SyncLiteStream}.
 *
 * <p>Manages a single JDBC connection to a SyncLite device, a prepared-statement
 * cache, and automatic schema evolution (ALTER TABLE ADD COLUMN) for insert
 * operations. Both {@code insert()} and {@code insertBatch()} are implemented here;
 * store-only operations (update, delete, select) live in {@link SyncLiteStore}.
 *
 * <p>All methods are {@code synchronized} on {@code this} so callers can safely
 * delegate from their own {@code synchronized} public methods.
 */
class SyncLiteTableWriter implements AutoCloseable {

    final Connection conn;
    // Ordered column names per table (lower-cased). LinkedHashSet preserves insertion
    // order so generated INSERT SQL column order is stable.
    final Map<String, LinkedHashSet<String>> tableColumns = new HashMap<>();
    // PreparedStatement cache.
    // Key for INSERT : table name (lower-cased)
    // Key for UPDATE : "table|U|setCol1,setCol2|whereCol1,whereCol2"
    // Key for DELETE : "table|D|whereCol1,whereCol2"
    // Key for SELECT : "table|S|whereCol1,whereCol2"
    final Map<String, PreparedStatement> stmtCache = new HashMap<>();
    // table name (lower) → set of cache keys for that table, used for bulk invalidation.
    final Map<String, Set<String>> tableToKeys = new HashMap<>();
    // SQL type used when a new String-valued column is auto-added via ALTER TABLE.
    private final String stringType;
    // Trusted JDBC handle for prepareTrustedStatement — may be null for Streaming devices.
    private final SyncLiteStoreConnection storeConn;
    // true when the entire instance was opened via openUnlogged().
    private final boolean allUnlogged;
    // Tables for which logging has been disabled via setTableLogging(..., false).
    private final Set<String> unloggedTables = new HashSet<>();

    SyncLiteTableWriter(Path dbPath, String urlPrefix, String stringType) throws SQLException {
        this(dbPath, urlPrefix, stringType, false);
    }

    SyncLiteTableWriter(Path dbPath, String urlPrefix, String stringType, boolean allUnlogged) throws SQLException {
        this.conn = DriverManager.getConnection(urlPrefix + dbPath.toAbsolutePath().toString());
        this.conn.setAutoCommit(true);
        this.stringType = stringType;
        this.allUnlogged = allUnlogged;
        // StreamingConnection does not extend SyncLiteStoreConnection; cast may be null.
        this.storeConn = (this.conn instanceof SyncLiteStoreConnection)
                ? (SyncLiteStoreConnection) this.conn : null;
    }

    // -------------------------------------------------------------------------
    // Schema
    // -------------------------------------------------------------------------

    synchronized void createTable(String table, Map<String, String> columnDefs) throws SQLException {
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
            String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
            String state = ex.getSQLState() == null ? "" : ex.getSQLState();
            if (!state.startsWith("X0Y32") && !msg.contains("already exists") && !msg.contains("already defined")) {
                throw ex;
            }
        }
        invalidateTable(table);
    }

    synchronized void dropTable(String table) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE " + table);
        } catch (SQLException ex) {
            String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
            String state = ex.getSQLState() == null ? "" : ex.getSQLState();
            if (!state.startsWith("42Y55") && !msg.contains("does not exist") && !msg.contains("not found") && !msg.contains("unknown table")) {
                throw ex;
            }
        }
        invalidateTable(table);
    }

    // -------------------------------------------------------------------------
    // DML — insert only
    // -------------------------------------------------------------------------

    synchronized void insert(String table, Map<String, Object> row) throws SQLException {
        if (isUnloggedFor(table)) { insertUnlogged(table, row); return; }
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

    synchronized void insertBatch(String table, List<Map<String, Object>> rows) throws SQLException {
        if (isUnloggedFor(table)) { insertBatchUnlogged(table, rows); return; }
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

    // -------------------------------------------------------------------------
    // Transaction control
    // -------------------------------------------------------------------------

    synchronized void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    synchronized void commit() throws SQLException {
        conn.commit();
    }

    synchronized void rollback() throws SQLException {
        conn.rollback();
        clearAllCaches();
    }

    // -------------------------------------------------------------------------
    // AutoCloseable
    // -------------------------------------------------------------------------

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
    // Internal helpers (package-private so SyncLiteStore can call them directly)
    // -------------------------------------------------------------------------

    void clearAllCaches() {
        for (PreparedStatement ps : stmtCache.values()) {
            try { ps.close(); } catch (SQLException ignored) {}
        }
        stmtCache.clear();
        tableColumns.clear();
        tableToKeys.clear();
    }

    /** Functional interface for SQL builders. */
    @FunctionalInterface
    interface SqlBuilder {
        String build() throws SQLException;
    }

    PreparedStatement getOrBuildStatement(String cacheKey, SqlBuilder sqlBuilder, String table)
            throws SQLException {
        PreparedStatement pstmt = stmtCache.get(cacheKey);
        if (pstmt == null || pstmt.isClosed()) {
            String sql = sqlBuilder.build();
            pstmt = (storeConn != null)
                    ? storeConn.prepareTrustedStatement(sql)
                    : conn.prepareStatement(sql);
            stmtCache.put(cacheKey, pstmt);
            tableToKeys.computeIfAbsent(table.toLowerCase(), k -> new HashSet<>()).add(cacheKey);
        }
        return pstmt;
    }

    PreparedStatement getInsertStatement(String table) throws SQLException {
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
            String sql = "INSERT INTO " + table + " (" + colSb + ") VALUES (" + phSb + ")";
            pstmt = (storeConn != null)
                    ? storeConn.prepareTrustedStatement(sql)
                    : conn.prepareStatement(sql);
            stmtCache.put(key, pstmt);
            tableToKeys.computeIfAbsent(key, k -> new HashSet<>()).add(key);
        }
        return pstmt;
    }

    void invalidateTable(String table) {
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

    Map<String, Object> normalizeKeys(Map<String, Object> row) {
        Map<String, Object> norm = new HashMap<>(row.size());
        for (Map.Entry<String, Object> e : row.entrySet()) {
            norm.put(e.getKey().toLowerCase(), e.getValue());
        }
        return norm;
    }

    void loadColumns(String table) throws SQLException {
        LinkedHashSet<String> cols = new LinkedHashSet<>();
        // Use Statement (not PreparedStatement) so that DBLoggerStatement.executeQuery(String)
        // is invoked rather than DBLoggerPreparedStatement.executeQuery(), which internally
        // calls the forbidden Statement.executeQuery(String) on a PreparedStatement.
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " WHERE 1=0")) {
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                cols.add(meta.getColumnLabel(i).toLowerCase());
            }
        }
        tableColumns.put(table.toLowerCase(), cols);
    }

    String inferSqlType(Object value) {
        if (value instanceof Long || value instanceof Integer ||
                value instanceof Short || value instanceof Byte ||
                value instanceof Boolean) return "INTEGER";
        if (value instanceof Double || value instanceof Float) return "REAL";
        if (value instanceof byte[]) return "BLOB";
        return stringType;
    }

    void ensureColumns(String table, Map<String, Object> colsWithValues) throws SQLException {
        String key = table.toLowerCase();
        if (!tableColumns.containsKey(key)) {
            try {
                loadColumns(table);
            } catch (SQLException ex) {
                // Table does not exist yet — auto-create it from the first row's column shape.
                String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
                if (!msg.contains("no such table") && !msg.contains("does not exist")
                        && !msg.contains("not found") && !msg.contains("unknown table")) {
                    throw ex;
                }
                LinkedHashMap<String, String> colDefs = new LinkedHashMap<>();
                for (Map.Entry<String, Object> e : colsWithValues.entrySet()) {
                    colDefs.put(e.getKey().toLowerCase(), inferSqlType(e.getValue()));
                }
                createTable(table, colDefs);
                loadColumns(table);
            }
        }
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
            Set<String> keys = tableToKeys.remove(key);
            if (keys != null) {
                for (String k : keys) {
                    PreparedStatement ps = stmtCache.remove(k);
                    if (ps != null) { try { ps.close(); } catch (SQLException ignored) {} }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Unlogged-write support
    // -------------------------------------------------------------------------

    /**
     * Marks {@code table} as logged (default) or unlogged.
     * Unlogged tables write data directly to the device DB but generate no
     * commandlog entries, so downstream CDC consumers do not see those writes.
     */
    void setTableLogging(String table, boolean logged) {
        if (logged) {
            unloggedTables.remove(table.toLowerCase());
        } else {
            unloggedTables.add(table.toLowerCase());
            invalidateTable(table); // discard any cached logged statements for this table
        }
    }

    boolean isUnloggedFor(String table) {
        return allUnlogged || unloggedTables.contains(table.toLowerCase());
    }

    /** Prepares a statement that bypasses the SyncLite logging layer. */
    private PreparedStatement prepareUnloggedPs(String sql) throws SQLException {
        if (storeConn != null) {
            return storeConn.prepareUnloggedStatement(sql);
        }
        // StreamingConnection extends DBLoggerConnection.
        return ((DBLoggerConnection) conn).prepareUnloggedStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * Commits the native (non-SyncLite) transaction — used after unlogged writes
     * so that data is durable without generating a commandlog entry.
     */
    void nativeCommit() throws SQLException {
        if (storeConn != null) {
            storeConn.superCommit();
        } else {
            ((DBLoggerConnection) conn).superCommit();
        }
    }

    PreparedStatement getUnloggedInsertStatement(String table) throws SQLException {
        String key = "~" + table.toLowerCase();
        PreparedStatement pstmt = stmtCache.get(key);
        if (pstmt == null || pstmt.isClosed()) {
            LinkedHashSet<String> cols = tableColumns.get(table.toLowerCase());
            StringBuilder colSb = new StringBuilder();
            StringBuilder phSb  = new StringBuilder();
            int i = 0;
            for (String col : cols) {
                if (i++ > 0) { colSb.append(", "); phSb.append(", "); }
                colSb.append(col);
                phSb.append("?");
            }
            String sql = "INSERT INTO " + table + " (" + colSb + ") VALUES (" + phSb + ")";
            pstmt = prepareUnloggedPs(sql);
            stmtCache.put(key, pstmt);
            tableToKeys.computeIfAbsent(table.toLowerCase(), k -> new HashSet<>()).add(key);
        }
        return pstmt;
    }

    PreparedStatement getUnloggedOrBuildStatement(String cacheKey, SqlBuilder sqlBuilder, String table)
            throws SQLException {
        String key = "~" + cacheKey;
        PreparedStatement pstmt = stmtCache.get(key);
        if (pstmt == null || pstmt.isClosed()) {
            String sql = sqlBuilder.build();
            pstmt = prepareUnloggedPs(sql);
            stmtCache.put(key, pstmt);
            tableToKeys.computeIfAbsent(table.toLowerCase(), k -> new HashSet<>()).add(key);
        }
        return pstmt;
    }

    synchronized void insertUnlogged(String table, Map<String, Object> row) throws SQLException {
        if (row == null || row.isEmpty()) throw new SQLException("row must contain at least one column");
        boolean prevAutoCommit = conn.getAutoCommit();
        if (prevAutoCommit) conn.setAutoCommit(false);
        try {
            ensureColumns(table, row);
            Map<String, Object> norm = normalizeKeys(row);
            PreparedStatement pstmt = getUnloggedInsertStatement(table);
            int pos = 1;
            for (String col : tableColumns.get(table.toLowerCase())) {
                pstmt.setObject(pos++, norm.get(col));
            }
            pstmt.executeUpdate();
            if (prevAutoCommit) nativeCommit();
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

    synchronized void insertBatchUnlogged(String table, List<Map<String, Object>> rows) throws SQLException {
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
            PreparedStatement pstmt = getUnloggedInsertStatement(table);
            LinkedHashSet<String> orderedCols = tableColumns.get(table.toLowerCase());
            for (Map<String, Object> row : rows) {
                Map<String, Object> norm = normalizeKeys(row);
                int pos = 1;
                for (String col : orderedCols) pstmt.setObject(pos++, norm.get(col));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if (prevAutoCommit) nativeCommit();
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

    List<Map<String, Object>> toList(ResultSet rs) throws SQLException {
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

    String getDefaultStringType() {
        return stringType;
    }
}
