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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Consolidator-module mirror of {@code io.synclite.StreamingTest}.
 * Disabled when {@code -DskipJunitDataConsolidation=true} is passed.
 */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class StreamingTestWithConsolidator {

    private static final String TEST_NAME   = "StreamingTestWithConsolidator";
    private static final String DEVICE_NAME = "streaming";

    private Path testDbPath;
    private Path testConfigPath;
    private DestinationOptions destination;

    @BeforeEach
    void setUp() throws Exception {
        ConsolidationTestSupport.resetTestDirs(TEST_NAME, DEVICE_NAME);
        testDbPath     = ConsolidationTestSupport.testRoot(TEST_NAME).resolve("test-streaming.db");
        testConfigPath = ConsolidationTestSupport.writeConfig(TEST_NAME);
        destination    = ConsolidationTestSupport.sqliteDestination(TEST_NAME);

        Class.forName("io.synclite.Streaming");
    }

    @AfterEach
    void tearDown() throws Exception {
        try { Streaming.closeDevice(testDbPath); } catch (SQLException ignored) {}
        Thread.sleep(150);
        System.out.println("\n[TEST ARTIFACTS PRESERVED] Location: " + ConsolidationTestSupport.testRoot(TEST_NAME) + "\n");
    }

    @Test
    void testBasicTableOperations() throws Exception {
        Streaming.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        String url = "jdbc:synclite_streaming:" + testDbPath;

        try (Connection conn = DriverManager.getConnection(url)) {
            SQLException protectedStmtEx;
            try (Statement stmt = conn.createStatement()) {
                protectedStmtEx = assertThrows(SQLException.class, () -> stmt.execute("DROP TABLE synclite_txn"));
            }
            assertTrue(protectedStmtEx.getMessage().contains("Protected internal table 'synclite_txn'"));

            SQLException protectedPstmtEx = assertThrows(SQLException.class,
                () -> conn.prepareStatement("DROP TABLE synclite_txn"));
            assertTrue(protectedPstmtEx.getMessage().contains("Protected internal table 'synclite_txn'"));

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
                assertTrue(rs.next());
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE streaming_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");
            }
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streaming_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 1); pstmt.setString(2, "test1"); pstmt.setInt(3, 100); pstmt.addBatch();
                pstmt.setInt(1, 2); pstmt.setString(2, "test2"); pstmt.setInt(3, 200); pstmt.addBatch();
                pstmt.executeBatch();
            }
        }

        Streaming.closeDevice(testDbPath);
        try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Streaming.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            SQLException updateEx = assertThrows(SQLException.class, () -> {
                conn.prepareStatement("UPDATE streaming_table SET value = ? WHERE name = ?");
            });
            assertTrue(updateEx.getMessage().contains("Unsupported SQL"));

            SQLException deleteEx = assertThrows(SQLException.class, () -> {
                conn.prepareStatement("DELETE FROM streaming_table WHERE name = ?");
            });
            assertTrue(deleteEx.getMessage().contains("Unsupported SQL"));

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO streaming_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 3); pstmt.setString(2, "test3"); pstmt.setInt(3, 300); pstmt.addBatch();
                pstmt.setInt(1, 4); pstmt.setString(2, "test4"); pstmt.setInt(3, 400); pstmt.addBatch();
                pstmt.executeBatch();
            }
            conn.commit();
        }

        ConsolidationTestSupport.awaitAndAssertDestinationRowCount(
                TEST_NAME, testDbPath, "streaming_table", 4);

        Streaming.closeDevice(testDbPath);

        long commitId;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + testDbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
            assertTrue(rs.next());
            commitId = rs.getLong(1);
            assertTrue(commitId > 0);
        }

        Path testStageDir = ConsolidationTestSupport.stageDir();
        assertTrue(Files.exists(testStageDir), "Stage directory should exist");

        Pattern sqllogPattern = Pattern.compile("^(\\d+)\\.sqllog$");
        Path latestLogFile = null;
        long latestMtime = -1;
        Path deviceStageDir = Files.list(testStageDir)
                .filter(p -> p.getFileName().toString().startsWith("synclite-" + DEVICE_NAME + "-"))
                .findFirst().orElse(testStageDir);
        try (var files = Files.walk(deviceStageDir)) {
            for (Path path : files.collect(Collectors.toList())) {
                if (!Files.isRegularFile(path)) continue;
                if (!sqllogPattern.matcher(path.getFileName().toString()).matches()) continue;
                long mtime = Files.getLastModifiedTime(path).toMillis();
                if (mtime > latestMtime) { latestMtime = mtime; latestLogFile = path; }
            }
        }
        assertNotNull(latestLogFile, "At least one .sqllog file should be created in stageDir");

        long foundCommitId;
        try (Connection logConn = DriverManager.getConnection("jdbc:sqlite:" + latestLogFile);
             Statement logStmt = logConn.createStatement();
             ResultSet logRs = logStmt.executeQuery("SELECT commit_id FROM commandlog ORDER BY change_number DESC LIMIT 1")) {
            assertTrue(logRs.next());
            foundCommitId = logRs.getLong("commit_id");
        }
        assertEquals(commitId, foundCommitId);
    }
}
