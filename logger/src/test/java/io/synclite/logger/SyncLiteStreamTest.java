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

import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link SyncLiteStream} / {@link Streaming} device.
 *
 * <p>All scenarios run sequentially inside a single {@code @Test} on one
 * persistent device.  The stageDir accumulates log segments across all phases
 * and is never wiped mid-test, so a consolidator pointed at the stageDir will
 * see the full, contiguous history.
 *
 * <p>The Streaming device does NOT persist inserted rows to the local SQLite
 * file – it is a write-ahead CDC log, not a queryable store.  Correctness
 * assertions are therefore commit-id cross-checks: after the device is closed
 * the commit_id in {@code synclite_txn} must match the latest commit_id
 * written to the stage log file.
 */
class SyncLiteStreamTest {

    @Test
    void testAllStreamingAPIs() throws Exception {
        Path testHome       = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("tests");
        Path testDbPath     = testHome.resolve("db").resolve("javalogger").resolve("SyncLiteStreamTest").resolve("test.db");
        Path testStageDir   = testHome.resolve("stageDir");
        Path testConfigPath = testDbPath.getParent().resolve("synclite.conf");

        // One-time cleanup from any previous run – never repeated between phases.
        for (int attempt = 0; attempt < 20 && Files.exists(testDbPath.getParent()); attempt++) {
            try { deleteRecursively(testDbPath.getParent()); break; }
            catch (IOException e) { Thread.sleep(200); }
        }
        if (Files.exists(testStageDir)) {
            try (var dirs = Files.list(testStageDir)) {
                dirs.filter(p -> p.getFileName().toString().startsWith("synclite-synclitestream-"))
                    .forEach(p -> { try { deleteRecursively(p); } catch (IOException ignored) {} });
            }
        }
        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);
        Files.writeString(testConfigPath,
                "local-data-stage-directory = " + testStageDir + "\ndevice-stage-type = FS\n");

        Class.forName("io.synclite.logger.Streaming");
        Streaming.initialize(testDbPath, testConfigPath, "synclitestream");

        // –– Phase 1: single insert ––––––––––––––––––––––––––––––––––––––––––
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("stream_events",
                    new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT", "user", "TEXT")));
            stream.insert("stream_events", Map.of("ts", 1000L, "type", "click", "user", "alice"));
        }

        // –– Phase 2: insert batch –––––––––––––––––––––––––––––––––––––––––––
        List<Map<String, Object>> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(Map.of("ts", (long) i, "type", "view", "user", "user" + i));
        }
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insertBatch("stream_events", batch);
        }

        // –– Phase 3: auto table creation ––––––––––––––––––––––––––––––––––––
        // Table is NOT pre-created – must be created automatically on first insert.
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insert("metrics", Map.of("name", "cpu", "value", 0.75));
        }

        // –– Phase 4: auto column addition –––––––––––––––––––––––––––––––––––
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("logs", new LinkedHashMap<>(Map.of("msg", "TEXT")));
            stream.insert("logs", Map.of("msg", "first"));
            // Second insert introduces new column "level" via ALTER TABLE – must not throw.
            stream.insert("logs", Map.of("msg", "second", "level", "INFO"));
        }

        // –– Phase 5: transactional commit –––––––––––––––––––––––––––––––––––
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("txn_events",
                    new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT")));
            stream.setAutoCommit(false);
            stream.insert("txn_events", Map.of("ts", 1L, "type", "A"));
            stream.insert("txn_events", Map.of("ts", 2L, "type", "B"));
            stream.commit();
        }

        // –– Phase 6: transactional rollback –––––––––––––––––––––––––––––––––
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            // First row – auto-committed.
            stream.insert("txn_events", Map.of("ts", 3L, "type", "good"));
            // Second row – rolled back; must not produce an additional log entry.
            stream.setAutoCommit(false);
            stream.insert("txn_events", Map.of("ts", 4L, "type", "bad"));
            stream.rollback();
        }

        // –– Phase 7: multiple tables independent ––––––––––––––––––––––––––––
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("clicks",
                    new LinkedHashMap<>(Map.of("url", "TEXT", "user", "TEXT")));
            stream.createTable("impressions",
                    new LinkedHashMap<>(Map.of("ad", "TEXT", "user", "TEXT")));
            stream.insert("clicks", Map.of("url", "/home", "user", "alice"));
            stream.insert("impressions", Map.of("ad", "banner1", "user", "bob"));
        }

        // –– Phase 8: batch with heterogeneous rows –––––––––––––––––––––––––––
        // Rows with different column sets – the union of columns must be used.
        List<Map<String, Object>> hetBatch = List.of(
                new LinkedHashMap<>(Map.of("a", 1L, "b", "x")),
                new LinkedHashMap<>(Map.of("a", 2L, "c", "y"))   // no "b", adds "c"
        );
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("mixed",
                    new LinkedHashMap<>(Map.of("a", "BIGINT", "b", "TEXT")));
            stream.insertBatch("mixed", hetBatch);
        }

        // –– Phase 9: create and drop table ––––––––––––––––––––––––––––––––––
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("tmp", new LinkedHashMap<>(Map.of("id", "BIGINT", "val", "TEXT")));
            stream.insert("tmp", Map.of("id", 1L, "val", "hello"));
            stream.dropTable("tmp");
            // Recreate with same name – must not throw.
            stream.createTable("tmp", new LinkedHashMap<>(Map.of("id", "BIGINT")));
        }

        // –– Phase 10: close is idempotent –––––––––––––––––––––––––––––––––––
        SyncLiteStream stream10 = SyncLiteStream.open(testDbPath);
        stream10.insert("stream_events", Map.of("ts", 9999L, "type", "idempotent", "user", "test"));
        stream10.close();
        // Second close must not throw.
        assertDoesNotThrow(stream10::close);

        // –– Phase 11: empty batch is no-op ––––––––––––––––––––––––––––––––––
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insertBatch("stream_events", List.of()); // must not throw
        }

        // –– Final validation –––––––––––––––––––––––––––––––––––––––––––––––––
        // Close the device to flush all pending log segments to stageDir, then
        // verify that the commit_id in synclite_txn matches the latest entry
        // in the accumulated stage log files.
        Streaming.closeAllDevices();
        Thread.sleep(150);

        long commitId;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + testDbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
            assertTrue(rs.next(), "synclite_txn must have a row");
            commitId = rs.getLong(1);
            assertTrue(commitId > 0, "commit_id must be positive after inserts");
        }

        Path latestLogFile = findLatestSqlLog(testStageDir);
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

    // ---
    // Helpers
    // ---

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
