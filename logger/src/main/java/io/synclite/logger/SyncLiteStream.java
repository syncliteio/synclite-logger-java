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
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple insert-only streaming API for SyncLite Streaming devices.
 *
 * <p>Provides {@link #insert} and {@link #insertBatch} on named tables backed by a
 * {@link Streaming} device. Every insert is captured to the SyncLite replication log
 * for downstream CDC consumption. No update, delete, or query operations are
 * available — the Streaming device is an append-only log.
 *
 * <p>Schema is managed automatically: on the first insert into a table the table is
 * created, and new columns are added via {@code ALTER TABLE ADD COLUMN} as they
 * appear in subsequent rows.
 *
 * <p>Usage:
 * <pre>
 *   Streaming.initialize(dbPath);
 *   try (SyncLiteStream stream = SyncLiteStream.open(dbPath)) {
 *       // single insert
 *       stream.insert("events", Map.of("ts", System.currentTimeMillis(), "type", "click", "user", "alice"));
 *
 *       // high-throughput batch
 *       stream.setAutoCommit(false);
 *       stream.insertBatch("events", batchOfRows);
 *       stream.commit();
 *   }
 * </pre>
 *
 * <p>The default auto-commit mode is {@code true}: each insert is committed
 * immediately. Call {@link #setAutoCommit(boolean) setAutoCommit(false)} to batch
 * multiple inserts into a single transaction and commit explicitly with
 * {@link #commit()}.
 *
 * <p><strong>Thread safety:</strong> All public methods are {@code synchronized}.
 * For maximum throughput open one {@code SyncLiteStream} per writer thread.
 * For transactional use ({@code setAutoCommit(false)}), do not share an instance
 * across threads.
 */
public class SyncLiteStream implements AutoCloseable {

    private static final String PREFIX = "jdbc:synclite_streaming:";

    // All shared write mechanics (connection, caches, insert, batch, txn) live here.
    private final SyncLiteTableWriter writer;

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Opens a {@code SyncLiteStream} against a previously-initialized Streaming
     * device at {@code dbPath}.
     *
     * <p>Call {@link Streaming#initialize(Path)} (or one of its overloads) at least
     * once before opening a stream.
     *
     * @param dbPath path to the Streaming device file
     * @return a new {@code SyncLiteStream}; the caller is responsible for closing it
     * @throws SQLException if the connection cannot be established
     */
    public static SyncLiteStream open(Path dbPath) throws SQLException {
        return new SyncLiteStream(dbPath, false);
    }

    /**
     * Opens a {@code SyncLiteStream} in which every write bypasses the SyncLite
     * replication log. Data is written directly to the device DB without generating
     * any commandlog entries. Useful for local-only scratch tables that should not
     * be replicated downstream.
     *
     * @param dbPath path to the Streaming device file
     */
    public static SyncLiteStream openUnlogged(Path dbPath) throws SQLException {
        return new SyncLiteStream(dbPath, true);
    }

    // -------------------------------------------------------------------------
    // Constructor (private — use open() / openUnlogged())
    // -------------------------------------------------------------------------

    private SyncLiteStream(Path dbPath, boolean allUnlogged) throws SQLException {
        this.writer = new SyncLiteTableWriter(dbPath, PREFIX, "TEXT", allUnlogged);
    }

    // -------------------------------------------------------------------------
    // Insert API
    // -------------------------------------------------------------------------

    /**
     * Inserts a single row into the named table.
     *
     * <p>If the table does not exist yet, it is created automatically from the
     * column names in {@code row}. If the table exists but is missing columns
     * present in {@code row}, those columns are added via {@code ALTER TABLE}.
     *
     * @param table table name
     * @param row   column name → value pairs. Use a {@link LinkedHashMap} to
     *              guarantee a predictable column order on first insert.
     * @throws SQLException if the insert fails
     */
    public synchronized void insert(String table, Map<String, Object> row) throws SQLException {
        writer.insert(table, row);
    }

    /**
     * Inserts multiple rows into the named table in a single batched operation.
     *
     * <p>If rows have different column sets, the union of all column names is used
     * and missing values are set to {@code null}.
     *
     * @param table table name
     * @param rows  list of rows; each row is a column name → value map
     * @throws SQLException if the batch insert fails
     */
    public synchronized void insertBatch(String table, List<Map<String, Object>> rows) throws SQLException {
        writer.insertBatch(table, rows);
    }

    // -------------------------------------------------------------------------
    // Schema API
    // -------------------------------------------------------------------------

    /**
     * Creates a table if it does not already exist.
     *
     * <p>The DDL is logged to the SyncLite replication log so downstream
     * consumers can materialise the schema before receiving any rows.
     *
     * @param table      table name
     * @param columnDefs ordered map of column name → SQL type
     *                   (e.g. {@code "TEXT"}, {@code "BIGINT"}).
     *                   Use a {@link java.util.LinkedHashMap} to guarantee column order.
     */
    public synchronized void createTable(String table, Map<String, String> columnDefs) throws SQLException {
        writer.createTable(table, columnDefs);
    }

    /**
     * Drops the named table if it exists.
     *
     * @param table table name
     */
    public synchronized void dropTable(String table) throws SQLException {
        writer.dropTable(table);
    }

    /**
     * Renames a table.
     *
     * <p>The DDL is logged to the SyncLite replication log so downstream
     * consumers can observe the table rename.
     *
     * @param oldTable current table name
     * @param newTable new table name
     */
    public synchronized void renameTable(String oldTable, String newTable) throws SQLException {
        writer.conn.createStatement().execute("ALTER TABLE " + oldTable + " RENAME TO " + newTable);
    }

    // -------------------------------------------------------------------------
    // Unlogged-write API
    // -------------------------------------------------------------------------

    /**
     * Disables or re-enables CDC logging for {@code table} on this stream instance.
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

    // -------------------------------------------------------------------------
    // Transaction control
    // -------------------------------------------------------------------------

    /**
     * Sets auto-commit mode. When {@code false}, subsequent inserts are not
     * committed until {@link #commit()} is called explicitly.
     *
     * <p>For transactional use, do not share this instance across threads.
     */
    public synchronized void setAutoCommit(boolean autoCommit) throws SQLException {
        writer.setAutoCommit(autoCommit);
    }

    /** Commits the current transaction. No-op when auto-commit is {@code true}. */
    public synchronized void commit() throws SQLException {
        writer.commit();
    }

    /**
     * Rolls back the current transaction and clears all caches.
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
}
