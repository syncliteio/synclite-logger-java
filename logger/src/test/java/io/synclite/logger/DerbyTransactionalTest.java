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

class DerbyTransactionalTest {

    private Path testDbPath;
    private Path testStageDir;
    private Path testConfigPath;

    @BeforeEach
    void setUp() throws Exception {
        Path testHome = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("tests");
        testDbPath = testHome.resolve("db").resolve("javalogger").resolve("DerbyTransactionalTest").resolve("test-derby.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testDbPath.getParent().resolve("synclite.conf");

        if (Files.exists(testDbPath.getParent())) {
            deleteRecursively(testDbPath.getParent());
        }
        if (Files.exists(testStageDir)) {
            try (var stageDirs = Files.list(testStageDir)) {
                stageDirs.filter(p -> p.getFileName().toString().startsWith("synclite-derbytransactional-"))
                         .forEach(p -> { try { deleteRecursively(p); } catch (java.io.IOException ignored) {} });
            }
        }

        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);

        String configContent = "local-data-stage-directory = " + testStageDir + "\n" +
                              "device-stage-type = FS\n";
        Files.writeString(testConfigPath, configContent);

        Class.forName("io.synclite.logger.Derby");
    }

    @AfterEach
    void tearDown() throws Exception {
        Derby.closeAllDevices();

        // Shut down Derby embedded engine to release db.lck file locks
        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (SQLException e) {
            // Derby always throws an exception on shutdown - this is expected
        }

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
        Derby.initialize(testDbPath, testConfigPath, "derbytransactional");

        String url = "jdbc:synclite_derby:" + testDbPath;

        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE derbytransactional_table (id INTEGER PRIMARY KEY, name VARCHAR(255), value INTEGER)");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO derbytransactional_table (id, name, value) VALUES (?, ?, ?)")) {
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
                 ResultSet rs = stmt.executeQuery("SELECT id, name, value FROM derbytransactional_table ORDER BY id")) {

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
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM derbytransactional_table")) {

                assertTrue(rs.next(), "Should have count result");
                assertEquals(2, rs.getInt("count"));
            }
        }

        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM derbytransactional_table")) {

                assertTrue(rs.next(), "Should have count result");
                assertEquals(2, rs.getInt("count"), "Two rows should exist after initial insert");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM derbytransactional_table ORDER BY id")) {

                assertTrue(rs.next(), "Should have first row");
                assertEquals("test1", rs.getString("name"));
                assertEquals(100, rs.getInt("value"));

                assertTrue(rs.next(), "Should have second row");
                assertEquals("test2", rs.getString("name"));
                assertEquals(200, rs.getInt("value"));

                assertFalse(rs.next(), "Should not have extra rows");
            }
        }

        Derby.closeDevice(testDbPath);

        // Shut down this Derby database to release file locks before re-initialize
        // Use DB-specific shutdown (not system shutdown) so the driver stays registered
        try {
            DriverManager.getConnection("jdbc:derby:" + testDbPath + ";shutdown=true");
        } catch (SQLException e) {
            // Derby always throws on shutdown - expected
        }

        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Derby.initialize(testDbPath, testConfigPath, "derbytransactional");

        long commitId = -1;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE derbytransactional_table SET value = ? WHERE name = ?")) {
                pstmt.setInt(1, 150);
                pstmt.setString(2, "test1");
                assertEquals(1, pstmt.executeUpdate(), "Should update one row");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM derbytransactional_table WHERE name = ?")) {
                pstmt.setString(1, "test2");
                assertEquals(1, pstmt.executeUpdate(), "Should delete one row");
            }

            conn.commit();

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM derbytransactional_table")) {
                assertTrue(rs.next(), "Should have count result");
                assertEquals(1, rs.getInt("count"), "Only one row should remain after update/delete");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM derbytransactional_table")) {
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

        Derby.closeAllDevices();

        assertTrue(Files.exists(testStageDir), "Stage directory should exist");

        Pattern sqllogPattern = Pattern.compile("^(\\d+)\\.sqllog$");
        Path lastLogFile = null;
        long maxSegNum = -1;

        Path deviceStageDir = Files.list(testStageDir).filter(p -> p.getFileName().toString().startsWith("synclite-derbytransactional-")).findFirst().orElse(testStageDir);
        try (var files = Files.walk(deviceStageDir)) {
            for (Path path : files.collect(Collectors.toList())) {
                if (!Files.isRegularFile(path)) {
                    continue;
                }
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
