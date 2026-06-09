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
 */

package io.synclite;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Consolidator-module mirror of
 * {@code io.synclite.SQLiteTransactionalTest}. Identical device
 * flow, but the initialize call carries a {@link DestinationOptions}
 * and the test waits for the in-process consolidator to apply the
 * final state to a SQLite destination DB before tearing down.
 *
 * <p>Disabled when {@code -DskipJunitDataConsolidation=true} is passed.
 */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class SQLiteTransactionalTestWithConsolidator {

    private static final String TEST_NAME   = "SQLiteTransactionalTestWithConsolidator";
    private static final String DEVICE_NAME = "sqlitetransactional";

    private Path testDbPath;
    private Path testConfigPath;
    private DestinationOptions destination;

    @BeforeEach
    void setUp() throws Exception {
        ConsolidationTestSupport.resetTestDirs(TEST_NAME, DEVICE_NAME);
        testDbPath     = ConsolidationTestSupport.testRoot(TEST_NAME).resolve("test-sqlite.db");
        testConfigPath = ConsolidationTestSupport.writeConfig(TEST_NAME);
        destination    = ConsolidationTestSupport.sqliteDestination(TEST_NAME);

        // Make sure the synclite JDBC driver is registered.
        Class.forName("io.synclite.SQLite");
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            SQLite.closeDevice(testDbPath);
        } catch (SQLException ignored) {
            // device may have been closed mid-test already
        }
        Thread.sleep(150);
        Path testHome = ConsolidationTestSupport.testRoot(TEST_NAME);
        System.out.println("\n[TEST ARTIFACTS PRESERVED] Location: " + testHome + "\n");
    }

    @Test
    void testBasicTableOperations() throws Exception {
        // Initialize SQLite device + in-process consolidator
        SQLite.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        String url = "jdbc:synclite_sqlite:" + testDbPath;

        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE sqlitetransactional_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO sqlitetransactional_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "test1");
                pstmt.setInt(3, 100);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setString(2, "test2");
                pstmt.setInt(3, 200);
                pstmt.addBatch();

                pstmt.executeBatch();
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, name, value FROM sqlitetransactional_table ORDER BY id")) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals(1, rs.getInt("id"));
                assertEquals("test1", rs.getString("name"));
                assertEquals(100, rs.getInt("value"));

                assertTrue(rs.next(), "Should have second row");
                assertEquals(2, rs.getInt("id"));
                assertEquals("test2", rs.getString("name"));
                assertEquals(200, rs.getInt("value"));

                assertFalse(rs.next(), "Should not have more rows");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM sqlitetransactional_table")) {
                assertTrue(rs.next(), "Should have count result");
                assertEquals(2, rs.getInt("count"));
            }
        }

        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM sqlitetransactional_table")) {
                assertTrue(rs.next(), "Should have count result");
                assertEquals(2, rs.getInt("count"), "Two rows should exist after initial insert");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM sqlitetransactional_table ORDER BY id")) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals("test1", rs.getString("name"));
                assertEquals(100, rs.getInt("value"));

                assertTrue(rs.next(), "Should have second row");
                assertEquals("test2", rs.getString("name"));
                assertEquals(200, rs.getInt("value"));

                assertFalse(rs.next(), "Should not have extra rows");
            }
        }

        // Close the device after first transaction (mirrors the logger test)
        SQLite.closeDevice(testDbPath);

        try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        SQLite.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        // Second transaction: update and delete data after reopen (data should persist)
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE sqlitetransactional_table SET value = ? WHERE name = ?")) {
                pstmt.setInt(1, 150);
                pstmt.setString(2, "test1");
                assertEquals(1, pstmt.executeUpdate(), "Should update one row");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM sqlitetransactional_table WHERE name = ?")) {
                pstmt.setString(1, "test2");
                assertEquals(1, pstmt.executeUpdate(), "Should delete one row");
            }

            conn.commit();
        }

        // Verify destination has the final consolidated state BEFORE
        // closing the device — awaitSync needs the consolidator alive.
        ConsolidationTestSupport.awaitAndAssertDestinationRowCount(
                TEST_NAME, testDbPath, "sqlitetransactional_table", 1);

        // Verify local final state and commit-id parity. The device
        // stays open so subsequent getConnection calls use it.
        long commitId;
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM sqlitetransactional_table")) {
                assertTrue(rs.next(), "Should have count result");
                assertEquals(1, rs.getInt("count"), "Only one row should remain after update/delete");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM sqlitetransactional_table")) {
                assertTrue(rs.next(), "Should have remaining row");
                assertEquals("test1", rs.getString("name"));
                assertEquals(150, rs.getInt("value"));
                assertFalse(rs.next(), "Should not have extra rows");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
                assertTrue(rs.next(), "Should have commit ID in synclite_txn table");
                commitId = rs.getLong(1);
                assertTrue(commitId > 0, "Commit ID should be positive");
            }
        }

        // Stage-log commit-id parity check (same as logger test).
        Path testStageDir = ConsolidationTestSupport.stageDir();
        assertTrue(Files.exists(testStageDir), "Stage directory should exist");

        long foundCommitId = -1;
        Pattern sqllogPattern = Pattern.compile("^(\\d+)\\.sqllog$");
        Path latestLogFile = null;
        long latestMtime = -1;

        Path deviceStageDir = Files.list(testStageDir)
                .filter(p -> p.getFileName().toString().startsWith("synclite-" + DEVICE_NAME + "-"))
                .findFirst().orElse(testStageDir);
        try (var files = Files.walk(deviceStageDir)) {
            for (Path path : files.collect(java.util.stream.Collectors.toList())) {
                if (!Files.isRegularFile(path)) continue;
                String fileName = path.getFileName().toString();
                if (!sqllogPattern.matcher(fileName).matches()) continue;
                long mtime = Files.getLastModifiedTime(path).toMillis();
                if (mtime > latestMtime) { latestMtime = mtime; latestLogFile = path; }
            }
        } catch (IOException e) {
            fail("Failed to traverse stageDir for .sqllog files: " + e.getMessage());
        }

        assertNotNull(latestLogFile, "At least one .sqllog file should be created in stageDir");

        String latestLogUrl = "jdbc:sqlite:" + latestLogFile;
        try (Connection logConn = DriverManager.getConnection(latestLogUrl);
             Statement logStmt = logConn.createStatement();
             ResultSet logRs = logStmt.executeQuery("SELECT commit_id FROM commandlog ORDER BY change_number DESC LIMIT 1")) {
            assertTrue(logRs.next(), "Latest stage log file must contain commandlog entry");
            foundCommitId = logRs.getLong("commit_id");
        }

        assertEquals(commitId, foundCommitId, "Commit ID in synclite_txn table should match the last logged transaction in newest stage log file");
    }
}
