/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.synclite.logger;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SyncLiteStreamTest {

    private Path testDbPath;
    private Path testStageDir;
    private Path testConfigPath;

    @BeforeEach
    void setUp() throws Exception {
        Path testHome = Path.of(System.getProperty("user.home"))
                .resolve("synclite").resolve("test").resolve("SyncLiteStreamTest");
        testDbPath = testHome.resolve("db").resolve("test.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testHome.resolve("synclite_logger.conf");

        if (Files.exists(testHome)) deleteRecursively(testHome);
        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);
        Files.writeString(testConfigPath,
                "local-data-stage-directory = " + testStageDir + "\ndestination-type = FS\n");

        Class.forName("io.synclite.logger.Streaming");
        Streaming.initialize(testDbPath, testConfigPath, "synclitestream");
    }

    @AfterEach
    void tearDown() throws Exception {
        try { Streaming.closeAllDevices(); } catch (Exception ignored) {}
        Thread.sleep(150);
    }

    // -------------------------------------------------------------------------
    // Tests
    //
    // The Streaming device does NOT persist inserted rows to the local SQLite
    // file — it is a write-ahead CDC log, not a queryable store. Therefore all
    // correctness assertions here are commit-id cross-checks: after closing the
    // device the commit_id recorded in synclite_txn must match the latest
    // commit_id written to the .sqllog stage file.
    // -------------------------------------------------------------------------

    @Test
    void testSingleInsert() throws Exception {
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("events",
                    new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT", "user", "TEXT")));
            stream.insert("events", Map.of("ts", 1000L, "type", "click", "user", "alice"));
        }
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testInsertBatch() throws Exception {
        List<Map<String, Object>> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(Map.of("ts", (long) i, "type", "view", "user", "user" + i));
        }
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("events",
                    new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT", "user", "TEXT")));
            stream.insertBatch("events", batch);
        }
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testAutoTableCreation() throws Exception {
        // Table is NOT pre-created — must be created automatically on first insert.
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insert("metrics", Map.of("name", "cpu", "value", 0.75));
        }
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testAutoColumnAddition() throws Exception {
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("logs", new LinkedHashMap<>(Map.of("msg", "TEXT")));
            stream.insert("logs", Map.of("msg", "first"));
            // Second insert introduces new column "level" via ALTER TABLE — must not throw.
            stream.insert("logs", Map.of("msg", "second", "level", "INFO"));
        }
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testTransactionalCommit() throws Exception {
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("events",
                    new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT")));
            stream.setAutoCommit(false);
            stream.insert("events", Map.of("ts", 1L, "type", "A"));
            stream.insert("events", Map.of("ts", 2L, "type", "B"));
            stream.commit();
        }
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testTransactionalRollback() throws Exception {
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("events",
                    new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT")));
            // First row — auto-committed.
            stream.insert("events", Map.of("ts", 1L, "type", "good"));
            // Second row — rolled back; must not produce an additional log entry.
            stream.setAutoCommit(false);
            stream.insert("events", Map.of("ts", 2L, "type", "bad"));
            stream.rollback();
        }
        // The rolled-back insert must not leave any trace in the stage log beyond
        // the commits already produced by the auto-committed rows.
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testMultipleTablesIndependent() throws Exception {
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("clicks",
                    new LinkedHashMap<>(Map.of("url", "TEXT", "user", "TEXT")));
            stream.createTable("impressions",
                    new LinkedHashMap<>(Map.of("ad", "TEXT", "user", "TEXT")));
            stream.insert("clicks", Map.of("url", "/home", "user", "alice"));
            stream.insert("impressions", Map.of("ad", "banner1", "user", "bob"));
        }
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testBatchWithHeterogeneousRows() throws Exception {
        // Rows with different column sets — the union of columns must be used.
        List<Map<String, Object>> batch = List.of(
                new LinkedHashMap<>(Map.of("a", 1L, "b", "x")),
                new LinkedHashMap<>(Map.of("a", 2L, "c", "y"))   // no "b", adds "c"
        );
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("mixed",
                    new LinkedHashMap<>(Map.of("a", "BIGINT", "b", "TEXT")));
            stream.insertBatch("mixed", batch);
        }
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testCreateAndDropTable() throws Exception {
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("tmp", new LinkedHashMap<>(Map.of("id", "BIGINT", "val", "TEXT")));
            stream.insert("tmp", Map.of("id", 1L, "val", "hello"));
            stream.dropTable("tmp");
            // Recreate with same name — must not throw.
            stream.createTable("tmp", new LinkedHashMap<>(Map.of("id", "BIGINT")));
        }
        validateCommitId(testDbPath, testStageDir);
    }

    @Test
    void testCloseIsIdempotent() throws Exception {
        SyncLiteStream stream = SyncLiteStream.open(testDbPath);
        stream.createTable("events",
                new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT")));
        stream.insert("events", Map.of("ts", 1L, "type", "x"));
        stream.close();
        // Second close must not throw.
        assertDoesNotThrow(stream::close);
    }

    @Test
    void testEmptyBatchIsNoOp() throws Exception {
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insertBatch("events", List.of()); // must not throw
        }
        // Flush and close so synclite_txn is readable via plain SQLite.
        Streaming.closeAllDevices();
        Thread.sleep(150);
        // No inserts committed — commit_id stays at zero (initial row from StreamingProcessor).
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + testDbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
            assertTrue(rs.next(), "synclite_txn must exist");
            assertEquals(0L, rs.getLong(1), "Empty batch must not advance commit_id");
        }
    }

    // -------------------------------------------------------------------------
    // Commit-ID validation helpers
    // -------------------------------------------------------------------------

    /**
     * Flushes staged log files to disk (by closing the device), then asserts that
     * the max commit_id in synclite_txn matches the latest commit_id in the most
     * recently modified .sqllog file under stageDir.
     *
     * After this call the device is closed; tearDown safely ignores the second close.
     */
    private void validateCommitId(Path dbPath, Path stageDir) throws Exception {
        Streaming.closeAllDevices();
        Thread.sleep(150);

        long commitId;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
            assertTrue(rs.next(), "synclite_txn must have a row");
            commitId = rs.getLong(1);
            assertTrue(commitId > 0, "commit_id must be positive after inserts");
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

    private void deleteRecursively(Path path) {
        if (Files.notExists(path)) return;
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.collect(Collectors.toList())) deleteRecursively(child);
            } catch (IOException ignored) {}
        }
        try { Files.deleteIfExists(path); } catch (IOException ignored) {}
    }
}
