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
 * Consolidator-module mirror of {@code io.synclite.DerbyStoreTest}.
 * Disabled when {@code -DskipJunitDataConsolidation=true} is passed.
 */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class DerbyStoreTestWithConsolidator {

    private static final String TEST_NAME   = "DerbyStoreTestWithConsolidator";
    private static final String DEVICE_NAME = "derbystore";

    private Path testDbPath;
    private Path testConfigPath;
    private DestinationOptions destination;

    @BeforeEach
    void setUp() throws Exception {
        ConsolidationTestSupport.resetTestDirs(TEST_NAME, DEVICE_NAME);
        testDbPath     = ConsolidationTestSupport.testRoot(TEST_NAME).resolve("test-derby-store.db");
        testConfigPath = ConsolidationTestSupport.writeConfig(TEST_NAME);
        destination    = ConsolidationTestSupport.sqliteDestination(TEST_NAME);

        Class.forName("io.synclite.DerbyStore");
    }

    @AfterEach
    void tearDown() throws Exception {
        try { DerbyStore.closeDevice(testDbPath); } catch (SQLException ignored) {}
        try { DriverManager.getConnection("jdbc:derby:" + testDbPath + ";shutdown=true"); }
        catch (SQLException expected) { /* Derby always throws on shutdown */ }
        Thread.sleep(150);
        System.out.println("\n[TEST ARTIFACTS PRESERVED] Location: " + ConsolidationTestSupport.testRoot(TEST_NAME) + "\n");
    }

    @Test
    void testBasicTableOperations() throws Exception {
        DerbyStore.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        String url = "jdbc:synclite_derby_store:" + testDbPath;

        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE derbystore_table (id INTEGER PRIMARY KEY, name VARCHAR(255), value INTEGER)");
            }
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO derbystore_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 1); pstmt.setString(2, "test1"); pstmt.setInt(3, 100); pstmt.addBatch();
                pstmt.setInt(1, 2); pstmt.setString(2, "test2"); pstmt.setInt(3, 200); pstmt.addBatch();
                pstmt.executeBatch();
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM derbystore_table")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt("count"));
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT name, value FROM derbystore_table ORDER BY id")) {
                assertTrue(rs.next()); assertEquals("test1", rs.getString("name")); assertEquals(100, rs.getInt("value"));
                assertTrue(rs.next()); assertEquals("test2", rs.getString("name")); assertEquals(200, rs.getInt("value"));
                assertFalse(rs.next());
            }
        }

        DerbyStore.closeDevice(testDbPath);
        try { DriverManager.getConnection("jdbc:derby:" + testDbPath + ";shutdown=true"); }
        catch (SQLException expected) {}
        try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        DerbyStore.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        long commitId = -1;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE derbystore_table SET value = ? WHERE name = ?")) {
                pstmt.setInt(1, 999); pstmt.setString(2, "test1"); pstmt.execute();
            }
            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM derbystore_table WHERE name = ?")) {
                pstmt.setString(1, "test2"); pstmt.execute();
            }
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO derbystore_table (id, name, value) VALUES (?, ?, ?)")) {
                pstmt.setInt(1, 3); pstmt.setString(2, "test3"); pstmt.setInt(3, 300); pstmt.addBatch();
                pstmt.setInt(1, 4); pstmt.setString(2, "test4"); pstmt.setInt(3, 400); pstmt.addBatch();
                pstmt.executeBatch();
            }
            conn.commit();

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM derbystore_table")) {
                assertTrue(rs.next()); assertEquals(3, rs.getInt("count"));
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT value FROM derbystore_table WHERE name = 'test1'")) {
                assertTrue(rs.next()); assertEquals(999, rs.getInt("value"));
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM derbystore_table WHERE name = 'test2'")) {
                assertTrue(rs.next()); assertEquals(0, rs.getInt("count"));
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
                assertTrue(rs.next()); commitId = rs.getLong(1); assertTrue(commitId > 0);
            }
        }

        ConsolidationTestSupport.awaitAndAssertDestinationRowCount(
                TEST_NAME, testDbPath, "derbystore_table", 3);

        DerbyStore.closeDevice(testDbPath);

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
