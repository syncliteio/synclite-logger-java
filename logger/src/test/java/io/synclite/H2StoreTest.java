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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class H2StoreTest {

    private Path testDbPath;
    private Path testStageDir;
    private Path testConfigPath;

    @BeforeEach
    void setUp() throws Exception {
        Path testHome = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("test").resolve("javalogger");
        testDbPath = testHome.resolve("db").resolve("H2StoreTest").resolve("test-h2-store.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testDbPath.getParent().resolve("synclite.conf");

        if (Files.exists(testDbPath.getParent())) {
            deleteRecursively(testDbPath.getParent());
        }
        if (Files.exists(testStageDir)) {
            try (var stageDirs = Files.list(testStageDir)) {
                stageDirs.filter(p -> p.getFileName().toString().startsWith("synclite-h2store-"))
                         .forEach(p -> { try { deleteRecursively(p); } catch (java.io.IOException ignored) {} });
            }
        }

        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);

        String configContent = "local-data-stage-directory = " + testStageDir + "\n" +
                              "device-stage-type = FS\n";
        Files.writeString(testConfigPath, configContent);

        Class.forName("io.synclite.H2Store");
    }

    @AfterEach
    void tearDown() throws Exception {
        H2Store.closeAllDevices();

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
        H2Store.initialize(testDbPath, testConfigPath, "h2store");

        String url = "jdbc:synclite_h2_store:" + testDbPath;

        // First transaction: create table and insert data
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE h2store_table (id INTEGER PRIMARY KEY, name VARCHAR(255), amount INTEGER)");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO h2store_table (id, name, amount) VALUES (?, ?, ?)")) {
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
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM h2store_table")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("count"));
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, amount FROM h2store_table ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("test1", rs.getString("name"));
                assertEquals(100, rs.getInt("amount"));
                assertTrue(rs.next());
                assertEquals("test2", rs.getString("name"));
                assertEquals(200, rs.getInt("amount"));
                assertFalse(rs.next());
            }
        }

        H2Store.closeDevice(testDbPath);

        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        H2Store.initialize(testDbPath, testConfigPath, "h2store");

        // Second transaction: UPDATE and DELETE are supported by Store devices
        long commitId = -1;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            // UPDATE test1's amount to 999
            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE h2store_table SET amount = ? WHERE name = ?")) {
                pstmt.setInt(1, 999);
                pstmt.setString(2, "test1");
                pstmt.execute();
            }

            // DELETE test2
            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM h2store_table WHERE name = ?")) {
                pstmt.setString(1, "test2");
                pstmt.execute();
            }

            // Insert additional rows
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO h2store_table (id, name, amount) VALUES (?, ?, ?)")) {
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

            // Validate 3 rows remain (test1 updated, test2 deleted, test3 and test4 inserted)
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM h2store_table")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt("count"), "Three rows should remain after update and delete");
            }

            // Validate test1 was updated
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT amount FROM h2store_table WHERE name = 'test1'")) {
                assertTrue(rs.next(), "test1 should still exist");
                assertEquals(999, rs.getInt("amount"), "test1 amount should be updated to 999");
            }

            // Validate test2 was deleted
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM h2store_table WHERE name = 'test2'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt("count"), "test2 should be deleted");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
                assertTrue(rs.next(), "Should have commit ID in synclite_txn table");
                commitId = rs.getLong(1);
                assertTrue(commitId > 0, "Commit ID should be positive");
            }
        }

        H2Store.closeAllDevices();

        // Stage log file validation
        assertTrue(Files.exists(testStageDir), "Stage directory should exist");

        Pattern sqllogPattern = Pattern.compile("^(\\d+)\\.sqllog$");
        Path lastLogFile = null;
        long maxSegNum = -1;

        Path deviceStageDir = Files.list(testStageDir).filter(p -> p.getFileName().toString().startsWith("synclite-h2store-")).findFirst().orElse(testStageDir);
        try (var files = Files.walk(deviceStageDir)) {
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
