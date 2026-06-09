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
 * Consolidator-module mirror of {@code io.synclite.DuckDBAppenderTest}.
 * Disabled when {@code -DskipJunitDataConsolidation=true} is passed.
 */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class DuckDBAppenderTestWithConsolidator {

    private static final String TEST_NAME   = "DuckDBAppenderTestWithConsolidator";
    private static final String DEVICE_NAME = "duckdbappender";

    private Path testDbPath;
    private Path testConfigPath;
    private DestinationOptions destination;

    @BeforeEach
    void setUp() throws Exception {
        ConsolidationTestSupport.resetTestDirs(TEST_NAME, DEVICE_NAME);
        testDbPath     = ConsolidationTestSupport.testRoot(TEST_NAME).resolve("test-duckdb-appender.db");
        testConfigPath = ConsolidationTestSupport.writeConfig(TEST_NAME);
        destination    = ConsolidationTestSupport.sqliteDestination(TEST_NAME);

        Class.forName("io.synclite.DuckDBAppender");
    }

    @AfterEach
    void tearDown() throws Exception {
        try { DuckDBAppender.closeDevice(testDbPath); } catch (SQLException ignored) {}
        Thread.sleep(150);
        System.out.println("\n[TEST ARTIFACTS PRESERVED] Location: " + ConsolidationTestSupport.testRoot(TEST_NAME) + "\n");
    }

    @Test
    void testBasicTableOperations() throws Exception {
        DuckDBAppender.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        String url = "jdbc:synclite_duckdb_appender:" + testDbPath;

        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE duckdbappender_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            }
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO duckdbappender_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 1); pstmt.setString(2, "test1"); pstmt.setInt(3, 100); pstmt.addBatch();
                pstmt.setInt(1, 2); pstmt.setString(2, "test2"); pstmt.setInt(3, 200); pstmt.addBatch();
                pstmt.executeBatch();
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM duckdbappender_table")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt("count"));
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM duckdbappender_table ORDER BY id")) {
                assertTrue(rs.next()); assertEquals("test1", rs.getString("name")); assertEquals(100, rs.getInt("value"));
                assertTrue(rs.next()); assertEquals("test2", rs.getString("name")); assertEquals(200, rs.getInt("value"));
                assertFalse(rs.next());
            }
        }

        DuckDBAppender.closeDevice(testDbPath);
        try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        DuckDBAppender.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        long commitId = -1;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            SQLException updateEx = assertThrows(SQLException.class, () -> {
                conn.prepareStatement("UPDATE duckdbappender_table SET value = ? WHERE name = ?");
            }, "Appender device should reject UPDATE");
            assertTrue(updateEx.getMessage().contains("Unsupported SQL"),
                    "Error message should indicate unsupported SQL, got: " + updateEx.getMessage());

            SQLException deleteEx = assertThrows(SQLException.class, () -> {
                conn.prepareStatement("DELETE FROM duckdbappender_table WHERE name = ?");
            }, "Appender device should reject DELETE");
            assertTrue(deleteEx.getMessage().contains("Unsupported SQL"),
                    "Error message should indicate unsupported SQL, got: " + deleteEx.getMessage());

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO duckdbappender_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 3); pstmt.setString(2, "test3"); pstmt.setInt(3, 300); pstmt.addBatch();
                pstmt.setInt(1, 4); pstmt.setString(2, "test4"); pstmt.setInt(3, 400); pstmt.addBatch();
                pstmt.executeBatch();
            }
            conn.commit();

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM duckdbappender_table")) {
                assertTrue(rs.next()); assertEquals(4, rs.getInt("count"));
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
                assertTrue(rs.next()); commitId = rs.getLong(1); assertTrue(commitId > 0);
            }
        }

        ConsolidationTestSupport.awaitAndAssertDestinationRowCount(
                TEST_NAME, testDbPath, "duckdbappender_table", 4);

        DuckDBAppender.closeDevice(testDbPath);

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
                assertTrue(logRs.next()); assertTrue(logRs.getInt("cnt") > 0);
            }
        }
    }
}
