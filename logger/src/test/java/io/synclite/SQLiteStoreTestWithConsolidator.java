/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Consolidator-module mirror of {@code io.synclite.SQLiteStoreTest}.
 * Disabled when {@code -DskipJunitDataConsolidation=true} is passed.
 */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class SQLiteStoreTestWithConsolidator {

    private static final String TEST_NAME   = "SQLiteStoreTestWithConsolidator";
    private static final String DEVICE_NAME = "sqlitestore";

    private Path testDbPath;
    private Path testConfigPath;
    private DestinationOptions destination;

    @BeforeEach
    void setUp() throws Exception {
        ConsolidationTestSupport.resetTestDirs(TEST_NAME, DEVICE_NAME);
        testDbPath     = ConsolidationTestSupport.testRoot(TEST_NAME).resolve("test-sqlite-store.db");
        testConfigPath = ConsolidationTestSupport.writeConfig(TEST_NAME);
        destination    = ConsolidationTestSupport.sqliteDestination(TEST_NAME);

        Class.forName("io.synclite.SQLiteStore");
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            SQLiteStore.closeDevice(testDbPath);
        } catch (SQLException ignored) {
        }
        Thread.sleep(150);
        System.out.println("\n[TEST ARTIFACTS PRESERVED] Location: " + ConsolidationTestSupport.testRoot(TEST_NAME) + "\n");
    }

    @Test
    void testBasicTableOperations() throws Exception {
        SQLiteStore.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        String url = "jdbc:synclite_sqlite_store:" + testDbPath;

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
                 ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
                assertTrue(rs.next(), "synclite_txn must still exist after blocked drop attempts");
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE sqlitestore_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO sqlitestore_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 1); pstmt.setString(2, "test1"); pstmt.setInt(3, 100); pstmt.addBatch();
                pstmt.setInt(1, 2); pstmt.setString(2, "test2"); pstmt.setInt(3, 200); pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM sqlitestore_table")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt("count"));
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM sqlitestore_table ORDER BY id")) {
                assertTrue(rs.next()); assertEquals("test1", rs.getString("name")); assertEquals(100, rs.getInt("value"));
                assertTrue(rs.next()); assertEquals("test2", rs.getString("name")); assertEquals(200, rs.getInt("value"));
                assertFalse(rs.next());
            }
        }

        SQLiteStore.closeDevice(testDbPath);
        try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        SQLiteStore.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        long commitId = -1;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE sqlitestore_table SET value = ? WHERE name = ?")) {
                pstmt.setInt(1, 999); pstmt.setString(2, "test1"); pstmt.execute();
            }
            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM sqlitestore_table WHERE name = ?")) {
                pstmt.setString(1, "test2"); pstmt.execute();
            }
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO sqlitestore_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 3); pstmt.setString(2, "test3"); pstmt.setInt(3, 300); pstmt.addBatch();
                pstmt.setInt(1, 4); pstmt.setString(2, "test4"); pstmt.setInt(3, 400); pstmt.addBatch();
                pstmt.executeBatch();
            }
            conn.commit();

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM sqlitestore_table")) {
                assertTrue(rs.next()); assertEquals(3, rs.getInt("count"), "Three rows should remain after update and delete");
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT value FROM sqlitestore_table WHERE name = 'test1'")) {
                assertTrue(rs.next(), "test1 should still exist"); assertEquals(999, rs.getInt("value"));
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM sqlitestore_table WHERE name = 'test2'")) {
                assertTrue(rs.next()); assertEquals(0, rs.getInt("count"), "test2 should be deleted");
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
                assertTrue(rs.next()); commitId = rs.getLong(1); assertTrue(commitId > 0);
            }
        }

        ConsolidationTestSupport.awaitAndAssertDestinationRowCount(
                TEST_NAME, testDbPath, "sqlitestore_table", 3);

        SQLiteStore.closeDevice(testDbPath);

        Path testStageDir = ConsolidationTestSupport.stageDir();
        assertTrue(Files.exists(testStageDir), "Stage directory should exist");

        Pattern sqllogPattern = Pattern.compile("^(\\d+)\\.sqllog$");
        Path lastLogFile = null;
        long maxSegNum = -1;
        Path deviceStageDir = Files.list(testStageDir)
                .filter(p -> p.getFileName().toString().startsWith("synclite-" + DEVICE_NAME + "-"))
                .findFirst().orElse(testStageDir);
        try (var files = Files.walk(deviceStageDir)) {
            for (Path path : files.collect(Collectors.toList())) {
                if (!Files.isRegularFile(path)) continue;
                Matcher m = sqllogPattern.matcher(path.getFileName().toString());
                if (m.matches()) {
                    long segNum = Long.parseLong(m.group(1));
                    if (segNum > maxSegNum) { maxSegNum = segNum; lastLogFile = path; }
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
