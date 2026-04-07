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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DuckDBAppenderTest {

    private Path testDbPath;
    private Path testStageDir;
    private Path testConfigPath;

    @BeforeEach
    void setUp() throws Exception {
        Path userHome = Path.of(System.getProperty("user.home"));
        Path syncLiteHome = userHome.resolve("synclite");
        Path testHome = syncLiteHome.resolve("test").resolve("DuckDBAppenderTest");
        testDbPath = testHome.resolve("db").resolve("test-duckdb-appender.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testHome.resolve("synclite_logger.conf");

        if (Files.exists(testHome)) {
            deleteRecursively(testHome);
        }

        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);

        String configContent = "local-data-stage-directory = " + testStageDir + "\n" +
                              "destination-type = FS\n";
        Files.writeString(testConfigPath, configContent);

        Class.forName("io.synclite.logger.DuckDBAppender");
    }

    @AfterEach
    void tearDown() throws Exception {
        DuckDBAppender.closeAllDevices();

        Thread.sleep(150);

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
        DuckDBAppender.initialize(testDbPath, testConfigPath, "duckdbappender");

        String url = "jdbc:synclite_duckdb_appender:" + testDbPath;

        // First transaction: create table and insert data
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)")) {
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

            // Validate data is stored locally
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM test_table")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("count"));
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM test_table ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("test1", rs.getString("name"));
                assertEquals(100, rs.getInt("value"));
                assertTrue(rs.next());
                assertEquals("test2", rs.getString("name"));
                assertEquals(200, rs.getInt("value"));
                assertFalse(rs.next());
            }
        }

        DuckDBAppender.closeDevice(testDbPath);

        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        DuckDBAppender.initialize(testDbPath, testConfigPath, "duckdbappender");

        // Second transaction: UPDATE and DELETE are not supported; insert more data after rejections
        long commitId = -1;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            SQLException updateEx = assertThrows(SQLException.class, () -> {
                conn.prepareStatement("UPDATE test_table SET value = ? WHERE name = ?");
            }, "Appender device should reject UPDATE");
            assertTrue(updateEx.getMessage().contains("Unsupported SQL"),
                    "Error message should indicate unsupported SQL, got: " + updateEx.getMessage());

            SQLException deleteEx = assertThrows(SQLException.class, () -> {
                conn.prepareStatement("DELETE FROM test_table WHERE name = ?");
            }, "Appender device should reject DELETE");
            assertTrue(deleteEx.getMessage().contains("Unsupported SQL"),
                    "Error message should indicate unsupported SQL, got: " + deleteEx.getMessage());

            // Insert additional data after rejections
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)")) {
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

            // Validate all 4 rows are present
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM test_table")) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt("count"), "Four rows should exist after second insert");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
                assertTrue(rs.next(), "Should have commit ID in synclite_txn table");
                commitId = rs.getLong(1);
                assertTrue(commitId > 0, "Commit ID should be positive");
            }
        }

        DuckDBAppender.closeAllDevices();

        // Stage log file validation
        assertTrue(Files.exists(testStageDir), "Stage directory should exist");

        Pattern sqllogPattern = Pattern.compile("^(\\d+)\\.sqllog$");
        Path lastLogFile = null;
        long maxSegNum = -1;

        try (var files = Files.walk(testStageDir)) {
            for (Path path : files.collect(Collectors.toList())) {
                if (!Files.isRegularFile(path)) continue;
                Matcher m = sqllogPattern.matcher(path.getFileName().toString());
                if (m.matches()) {
                    long segNum = Long.parseLong(m.group(1));
                    if (segNum > maxSegNum) {
                        maxSegNum = segNum;
                        lastLogFile = path;
                    }
                }
            }
        }

        assertNotNull(lastLogFile, "At least one .sqllog file should exist in stageDir");

        try (Connection logConn = DriverManager.getConnection("jdbc:sqlite:" + lastLogFile);
             PreparedStatement logStmt = logConn.prepareStatement(
                     "SELECT COUNT(*) as cnt FROM commandlog WHERE commit_id = ?")) {
            logStmt.setLong(1, commitId);
            try (ResultSet logRs = logStmt.executeQuery()) {
                assertTrue(logRs.next(), "Stage log query must return a result");
                assertTrue(logRs.getInt("cnt") > 0,
                        "Last stage log file must contain an entry for commit_id " + commitId);
            }
        }
    }
}
