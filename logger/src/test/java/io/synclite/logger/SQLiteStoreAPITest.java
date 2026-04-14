/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.synclite.logger;

import static org.junit.jupiter.api.Assertions.*;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SQLiteStoreAPITest {

    private Path testDbPath;
    private Path testStageDir;
    private Path testConfigPath;

    @BeforeEach
    void setUp() throws Exception {
        Path testHome = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("test");
        testDbPath = testHome.resolve("db").resolve("SQLiteStoreAPITest").resolve("test.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testDbPath.getParent().resolve("synclite_logger.conf");

        if (Files.exists(testDbPath.getParent())) deleteRecursively(testDbPath.getParent());
        if (Files.exists(testStageDir)) {
            try (var stageDirs = Files.list(testStageDir)) {
                stageDirs.filter(p -> p.getFileName().toString().startsWith("synclite-sqlitestoreapi-"))
                         .forEach(p -> { try { deleteRecursively(p); } catch (java.io.IOException ignored) {} });
            }
        }
        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);
        Files.writeString(testConfigPath,
                "local-data-stage-directory = " + testStageDir + "\ndestination-type = FS\n");

        Class.forName("io.synclite.logger.SQLiteStore");
        SQLiteStore.initialize(testDbPath, testConfigPath, "sqlitestoreapi");
    }

    @AfterEach
    void tearDown() throws Exception {
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
    }

    @Test
    void testAllAPIs() throws Exception {
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            runAPITest(store, "sqlitestoreapi_players");
        }
        // Flush logs, then cross-check commit_id between synclite_txn and the stage log file.
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
        validateCommitId(testDbPath, testStageDir);
    }

    static void runAPITest(SyncLiteStore store, String tableName) throws Exception {
        // --- createTable ---
        Map<String, String> cols = new LinkedHashMap<>();
        cols.put("id", "INTEGER PRIMARY KEY");
        cols.put("name", "VARCHAR(255)");
        cols.put("score", "INTEGER");
        store.createTable(tableName, cols);

        // --- insert single row ---
        store.insert(tableName, Map.of("id", 1, "name", "Alice", "score", 100));
        store.insert(tableName, Map.of("id", 2, "name", "Bob", "score", 200));

        // --- selectAll ---
        List<Map<String, Object>> rows = store.selectAll(tableName);
        assertEquals(2, rows.size());

        // --- select with where ---
        List<Map<String, Object>> aliceRows = store.select(tableName, Map.of("name", "Alice"));
        assertEquals(1, aliceRows.size());
        assertEquals(100, ((Number) aliceRows.get(0).get("score")).intValue());

        // --- update ---
        store.update(tableName, Map.of("score", 999), Map.of("name", "Alice"));
        List<Map<String, Object>> updated = store.select(tableName, Map.of("name", "Alice"));
        assertEquals(999, ((Number) updated.get(0).get("score")).intValue());

        // --- delete ---
        store.delete(tableName, Map.of("name", "Bob"));
        assertEquals(1, store.selectAll(tableName).size());

        // --- insertBatch ---
        List<Map<String, Object>> batch = List.of(
                Map.of("id", 3, "name", "Carol", "score", 300),
                Map.of("id", 4, "name", "Dave", "score", 400)
        );
        store.insertBatch(tableName, batch);
        assertEquals(3, store.selectAll(tableName).size());

        // --- updateBatch ---
        store.updateBatch(tableName,
                List.of(Map.of("score", 350), Map.of("score", 450)),
                List.of(Map.of("name", "Carol"), Map.of("name", "Dave")));
        assertEquals(350, ((Number) store.select(tableName, Map.of("name", "Carol")).get(0).get("score")).intValue());
        assertEquals(450, ((Number) store.select(tableName, Map.of("name", "Dave")).get(0).get("score")).intValue());

        // --- deleteBatch ---
        store.deleteBatch(tableName, List.of(Map.of("name", "Carol"), Map.of("name", "Dave")));
        assertEquals(1, store.selectAll(tableName).size());

        // --- auto column addition on insert ---
        // Insert a row with a brand-new column "level" that doesn't exist yet.
        store.insert(tableName, Map.of("id", 5, "name", "Eve", "score", 500, "level", 7));
        List<Map<String, Object>> eveRows = store.select(tableName, Map.of("name", "Eve"));
        assertEquals(1, eveRows.size());
        assertEquals(7, ((Number) eveRows.get(0).get("level")).intValue());

        // --- auto column addition on update ---
        // Update with a new column "badge" that doesn't exist yet.
        store.update(tableName, Map.of("badge", "gold"), Map.of("name", "Eve"));
        List<Map<String, Object>> eveUpdated = store.select(tableName, Map.of("name", "Eve"));
        assertEquals("gold", eveUpdated.get(0).get("badge"));

        // --- transactional insert + rollback ---
        store.setAutoCommit(false);
        store.insert(tableName, Map.of("id", 99, "name", "Temp", "score", 0));
        store.rollback();
        assertTrue(store.select(tableName, Map.of("name", "Temp")).isEmpty(),
                "Rolled-back row must not be visible");

        // --- transactional insert + commit ---
        store.setAutoCommit(false);
        store.insert(tableName, Map.of("id", 6, "name", "Frank", "score", 600));
        store.commit();
        assertEquals(1, store.select(tableName, Map.of("name", "Frank")).size());

        // --- dropTable ---
        store.dropTable(tableName);
        // Table is gone; a fresh createTable must succeed
        store.createTable(tableName, Map.of("id", "INTEGER PRIMARY KEY"));
        assertEquals(0, store.selectAll(tableName).size());
    }

    // -------------------------------------------------------------------------
    // Commit-ID validation helpers
    // -------------------------------------------------------------------------

    /**
     * Asserts that the max commit_id in synclite_txn matches the latest commit_id
     * in the most recently modified .sqllog file under stageDir.
     */
    private void validateCommitId(Path dbPath, Path stageDir) throws Exception {
        long commitId;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
            assertTrue(rs.next(), "synclite_txn must have a row");
            commitId = rs.getLong(1);
            assertTrue(commitId > 0, "commit_id must be positive after store operations");
        }

        Path latestLogFile = findLatestSqlLog(stageDir);
        assertNotNull(latestLogFile, "At least one .sqllog file must be created in stageDir");

        long foundCommitId;
        try (Connection logConn = DriverManager.getConnection("jdbc:sqlite:" + latestLogFile);
             Statement logStmt = logConn.createStatement();
             ResultSet logRs = logStmt.executeQuery(
                     "SELECT commit_id FROM commandlog ORDER BY change_number DESC LIMIT 1")) {
            assertTrue(logRs.next(), "Latest stage log must contain a commandlog entry");
            foundCommitId = logRs.getLong("commit_id");
        }

        assertEquals(commitId, foundCommitId,
                "commit_id in synclite_txn must match the latest commit in the stage log file");
    }

    private Path findLatestSqlLog(Path stageDir) throws IOException {
        Pattern pattern = Pattern.compile("^\\d+\\.sqllog$");
        Path latest = null;
        long latestMtime = -1;
        try (var stream = Files.walk(stageDir)) {
            for (Path p : stream.collect(Collectors.toList())) {
                if (!Files.isRegularFile(p)) continue;
                if (!pattern.matcher(p.getFileName().toString()).matches()) continue;
                long mtime = Files.getLastModifiedTime(p).toMillis();
                if (mtime > latestMtime) { latestMtime = mtime; latest = p; }
            }
        }
        return latest;
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.notExists(path)) return;
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.collect(Collectors.toList())) deleteRecursively(child);
            }
        }
        Files.deleteIfExists(path);
    }
}
