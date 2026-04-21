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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamingTest {

    private Path testDbPath;
    private Path testStageDir;
    private Path testConfigPath;

    @BeforeEach
    void setUp() throws Exception {
        Path testHome = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("test");
        testDbPath = testHome.resolve("db").resolve("StreamingTest").resolve("test-streaming.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testDbPath.getParent().resolve("synclite_logger.conf");

        // Clean up previous test state before each run
        if (Files.exists(testDbPath.getParent())) {
            deleteRecursively(testDbPath.getParent());
        }
        if (Files.exists(testStageDir)) {
            try (var stageDirs = Files.list(testStageDir)) {
                stageDirs.filter(p -> p.getFileName().toString().startsWith("synclite-streaming-"))
                         .forEach(p -> { try { deleteRecursively(p); } catch (java.io.IOException ignored) {} });
            }
        }

        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);

        // Create a basic config file
        String configContent = "local-data-stage-directory = " + testStageDir + "\n" +
                              "destination-type = FS\n";
        Files.writeString(testConfigPath, configContent);

        // Load the Streaming driver
        Class.forName("io.synclite.logger.Streaming");
    }

    @AfterEach
    void tearDown() throws Exception {
        // Clean up test files and directories
        Streaming.closeAllDevices();

        // Give Windows some time to release locks after closeAllDevices
        Thread.sleep(150);

        // Preserve test artifacts for analysis (both on success and failure)
        Path testHome = testStageDir.getParent();
        System.out.println("\n[TEST ARTIFACTS PRESERVED] Location: " + testHome + "\n");
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
        // Initialize Streaming device
        Streaming.initialize(testDbPath, testConfigPath, "streaming");

        String url = "jdbc:synclite_streaming:" + testDbPath;

        // First transaction: create table and insert data
        try (Connection conn = DriverManager.getConnection(url)) {
            SQLException protectedStmtEx;
            try (Statement stmt = conn.createStatement()) {
                protectedStmtEx = assertThrows(SQLException.class, () -> stmt.execute("DROP TABLE synclite_txn"));
            }
            assertTrue(protectedStmtEx.getMessage().contains("Protected internal table 'synclite_txn'"),
                    "Statement drop should be blocked with protected-table message");

                SQLException protectedPstmtEx = assertThrows(SQLException.class,
                    () -> conn.prepareStatement("DROP TABLE synclite_txn"));
                assertTrue(protectedPstmtEx.getMessage().contains("Protected internal table 'synclite_txn'"),
                    "PreparedStatement drop should be blocked with protected-table message");

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT commit_id FROM synclite_txn")) {
                assertTrue(rs.next(), "synclite_txn must still exist after blocked drop attempts");
            }

            // Create table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE streaming_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            }

            // Insert data
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streaming_table (id, name, value) VALUES (?, ?, ?)")) {
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

            // No data validation: streaming device does not persist data locally
        }

        // Force flush and close the device after first transaction
        Streaming.closeDevice(testDbPath);

        // Give Windows some time to release locks after closeDevice
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Streaming.initialize(testDbPath, testConfigPath, "streaming");

        // Second transaction: streaming device does not support UPDATE or DELETE;
        // verify that both are rejected, then insert new data
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            // Attempt UPDATE - streaming device allows only DDL and INSERT.
            // Use ? params so DBLogger parent validation passes and StreamingPreparedStatement throws its own error.
            SQLException updateEx = assertThrows(SQLException.class, () -> {
                conn.prepareStatement("UPDATE streaming_table SET value = ? WHERE name = ?");
            }, "Streaming device should reject UPDATE");
            assertTrue(updateEx.getMessage().contains("Unsupported SQL"),
                    "Error message should indicate unsupported SQL, got: " + updateEx.getMessage());

            // Attempt DELETE - streaming device allows only DDL and INSERT.
            // Use ? params so DBLogger parent validation passes and StreamingPreparedStatement throws its own error.
            SQLException deleteEx = assertThrows(SQLException.class, () -> {
                conn.prepareStatement("DELETE FROM streaming_table WHERE name = ?");
            }, "Streaming device should reject DELETE");
            assertTrue(deleteEx.getMessage().contains("Unsupported SQL"),
                    "Error message should indicate unsupported SQL, got: " + deleteEx.getMessage());

            // After rejections, insert additional data
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streaming_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 3);
                pstmt.setString(2, "test3");
                pstmt.setInt(3, 300);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setString(2, "test4");
                pstmt.setInt(3, 400);
                pstmt.addBatch();

                pstmt.executeBatch();
            }

            conn.commit();
        }

        // Force flush and close all devices after second transaction
        Streaming.closeAllDevices();

        // Commit ID validation: synclite_txn is maintained by the streaming device
        // even though no data is persisted locally in test_table
        long commitId;
        try (Connection conn = DriverManager.getConnection(url)) {
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

        Path deviceStageDir = Files.list(testStageDir).filter(p -> p.getFileName().toString().startsWith("synclite-streaming-")).findFirst().orElse(testStageDir);
        try (var files = Files.walk(deviceStageDir)) {
            for (Path path : files.collect(Collectors.toList())) {
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
    }
}
