/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.synclite;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Consolidator-module mirror of {@code io.synclite.SyncLiteStreamTest}.
 * Disabled when {@code -DskipJunitDataConsolidation=true} is passed.
 */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class SyncLiteStreamTestWithConsolidator {

    private static final String TEST_NAME   = "SyncLiteStreamTestWithConsolidator";
    private static final String DEVICE_NAME = "synclitestream";

    @Test
    void testAllStreamingAPIs() throws Exception {
        ConsolidationTestSupport.resetTestDirs(TEST_NAME, DEVICE_NAME);
        Path testDbPath     = ConsolidationTestSupport.testRoot(TEST_NAME).resolve("test.db");
        Path testConfigPath = ConsolidationTestSupport.writeConfig(TEST_NAME);
        Path testStageDir   = ConsolidationTestSupport.stageDir();
        DestinationOptions destination = ConsolidationTestSupport.sqliteDestination(TEST_NAME);

        Class.forName("io.synclite.Streaming");
        Streaming.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);

        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("stream_events",
                    new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT", "user", "TEXT")));
            stream.insert("stream_events", Map.of("ts", 1000L, "type", "click", "user", "alice"));
        }

        List<Map<String, Object>> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(Map.of("ts", (long) i, "type", "view", "user", "user" + i));
        }
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insertBatch("stream_events", batch);
        }

        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insert("metrics", Map.of("name", "cpu", "value", 0.75));
        }

        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("logs", new LinkedHashMap<>(Map.of("msg", "TEXT")));
            stream.insert("logs", Map.of("msg", "first"));
            stream.insert("logs", Map.of("msg", "second", "level", "INFO"));
        }

        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("txn_events",
                    new LinkedHashMap<>(Map.of("ts", "BIGINT", "type", "TEXT")));
            stream.setAutoCommit(false);
            stream.insert("txn_events", Map.of("ts", 1L, "type", "A"));
            stream.insert("txn_events", Map.of("ts", 2L, "type", "B"));
            stream.commit();
        }

        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insert("txn_events", Map.of("ts", 3L, "type", "good"));
            stream.setAutoCommit(false);
            stream.insert("txn_events", Map.of("ts", 4L, "type", "bad"));
            stream.rollback();
        }

        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("clicks",
                    new LinkedHashMap<>(Map.of("url", "TEXT", "user", "TEXT")));
            stream.createTable("impressions",
                    new LinkedHashMap<>(Map.of("ad", "TEXT", "user", "TEXT")));
            stream.insert("clicks", Map.of("url", "/home", "user", "alice"));
            stream.insert("impressions", Map.of("ad", "banner1", "user", "bob"));
        }

        List<Map<String, Object>> hetBatch = List.of(
                new LinkedHashMap<>(Map.of("a", 1L, "b", "x")),
                new LinkedHashMap<>(Map.of("a", 2L, "c", "y"))
        );
        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("mixed",
                    new LinkedHashMap<>(Map.of("a", "BIGINT", "b", "TEXT")));
            stream.insertBatch("mixed", hetBatch);
        }

        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.createTable("tmp", new LinkedHashMap<>(Map.of("id", "BIGINT", "val", "TEXT")));
            stream.insert("tmp", Map.of("id", 1L, "val", "hello"));
            stream.dropTable("tmp");
            stream.createTable("tmp", new LinkedHashMap<>(Map.of("id", "BIGINT")));
        }

        SyncLiteStream stream10 = SyncLiteStream.open(testDbPath);
        stream10.insert("stream_events", Map.of("ts", 9999L, "type", "idempotent", "user", "test"));
        stream10.close();
        assertDoesNotThrow(stream10::close);

        try (SyncLiteStream stream = SyncLiteStream.open(testDbPath)) {
            stream.insertBatch("stream_events", List.of());
        }

        // Verify consolidation: stream_events accumulates 1 + 5 + 1 = 7 rows.
        ConsolidationTestSupport.awaitAndAssertDestinationRowCount(
                TEST_NAME, testDbPath, "stream_events", 7);

        try { Streaming.closeDevice(testDbPath); } catch (SQLException ignored) {}
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
}
