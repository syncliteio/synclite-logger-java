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

package io.synclite.logger;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SQLiteTransactionalTest {

    private Path testDbPath;
    private Path testStageDir;
    private Path testConfigPath;
    private boolean testPassed = false;

    @BeforeEach
    void setUp() throws Exception {
        // Create directories in user home following SyncLite pattern
        Path userHome = Path.of(System.getProperty("user.home"));
        Path syncLiteHome = userHome.resolve("synclite");
        Path testHome = syncLiteHome.resolve("test");
        testDbPath = testHome.resolve("db").resolve("test-sqlite.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testHome.resolve("synclite_logger.conf");

        // Clean up previous test state before each run
        if (Files.exists(testHome)) {
            deleteRecursively(testHome);
        }

        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);

        // Create a basic config file
        String configContent = "local-data-stage-directory = " + testStageDir + "\n" +
                              "destination-type = FS\n";
        Files.writeString(testConfigPath, configContent);

        // Load the SQLite driver
        Class.forName("io.synclite.logger.SQLite");
    }

    @AfterEach
    void tearDown() throws Exception {
        // Clean up test files and directories
        SQLite.closeAllDevices();

        // Give Windows some time to release locks after closeAllDevices
        Thread.sleep(150);

        // Preserve test artifacts for analysis (both on success and failure)
        Path testHome = testStageDir.getParent(); // ~/synclite/test
        System.out.println("\n[TEST ARTIFACTS PRESERVED] Location: " + testHome + "\n");
        
        // Reset flag for next test
        testPassed = false;
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.notExists(path)) {
            return;
        }
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.collect(Collectors.toList())) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    @Test
    void testBasicTableOperations() throws SQLException, IOException {
        // Initialize SQLite device
        SQLite.initialize(testDbPath, testConfigPath, "sqlitetransactional");

        String url = "jdbc:synclite_sqlite:" + testDbPath;

        try (Connection conn = DriverManager.getConnection(url)) {
            // Create table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            }

            // Insert data
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test_table (name, value) VALUES (?, ?)")) {
                pstmt.setString(1, "test1");
                pstmt.setInt(2, 100);
                pstmt.addBatch();

                pstmt.setString(1, "test2");
                pstmt.setInt(2, 200);
                pstmt.addBatch();

                pstmt.executeBatch();
            }

            // Read data back
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, name, value FROM test_table ORDER BY id")) {

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

            // Verify row count
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM test_table")) {

                assertTrue(rs.next(), "Should have count result");
                assertEquals(2, rs.getInt("count"));
            }
        }

        // Verify the data is in the database after first transaction
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM test_table")) {

                assertTrue(rs.next(), "Should have count result");
                assertEquals(2, rs.getInt("count"), "Two rows should exist after initial insert");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM test_table ORDER BY id")) {

                assertTrue(rs.next(), "Should have first row");
                assertEquals("test1", rs.getString("name"));
                assertEquals(100, rs.getInt("value"));

                assertTrue(rs.next(), "Should have second row");
                assertEquals("test2", rs.getString("name"));
                assertEquals(200, rs.getInt("value"));

                assertFalse(rs.next(), "Should not have extra rows");
            }
        }

        // Force flush and close the device after first transaction
        SQLite.closeDevice(testDbPath);

        // Give Windows some time to release locks after closeAllDevices
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        SQLite.initialize(testDbPath, testConfigPath, "sqlitetransactional");

        // Second transaction: update and delete data after reopen (data should persist)
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE test_table SET value = ? WHERE name = ?")) {
                pstmt.setInt(1, 150);
                pstmt.setString(2, "test1");
                assertEquals(1, pstmt.executeUpdate(), "Should update one row");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM test_table WHERE name = ?")) {
                pstmt.setString(1, "test2");
                assertEquals(1, pstmt.executeUpdate(), "Should delete one row");
            }

            conn.commit();
        }

        // Force flush and close all devices after second transaction
        SQLite.closeAllDevices();

        // Verify final state in the database after second transaction
        long commitId;
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM test_table")) {
                assertTrue(rs.next(), "Should have count result");
                assertEquals(1, rs.getInt("count"), "Only one row should remain after update/delete");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM test_table")) {
                assertTrue(rs.next(), "Should have remaining row");
                assertEquals("test1", rs.getString("name"));
                assertEquals(150, rs.getInt("value"));
                assertFalse(rs.next(), "Should not have extra rows");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT commit_id FROM synclite_txn")) {
                assertTrue(rs.next(), "Should have commit ID in synclite_txn table");
                commitId = rs.getLong("commit_id");
                assertTrue(commitId > 0, "Commit ID should be positive");
            }
        }

        // Find log files in the stage directory created by closeAllDevices()
        assertTrue(Files.exists(testStageDir), "Stage directory should exist");

            // Find the most recently updated .sqllog in stageDir and read its latest commit_id
            long foundCommitId = -1;
            Pattern sqllogPattern = Pattern.compile("^(\\d+)\\.sqllog$");
            Path latestLogFile = null;
            long latestMtime = -1;

            try (var files = Files.walk(testStageDir)) {
                for (Path path : files.collect(java.util.stream.Collectors.toList())) {
                    if (!Files.isRegularFile(path)) {
                        continue;
                    }
                    String fileName = path.getFileName().toString();
                    if (!sqllogPattern.matcher(fileName).matches()) {
                        continue;
                    }

                    long mtime = Files.getLastModifiedTime(path).toMillis();
                    if (mtime > latestMtime) {
                        latestMtime = mtime;
                        latestLogFile = path;
                    }
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
            
            // Test passed - cleanup will delete artifacts
            testPassed = true;
    }
}