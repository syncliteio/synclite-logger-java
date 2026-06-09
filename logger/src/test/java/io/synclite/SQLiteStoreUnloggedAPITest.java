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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * Tests for the unlogged-write API: {@link SyncLiteStore#openUnlogged},
 * {@link SyncLiteStore#setTableLogging}, {@link SyncLiteStore#insertUnlogged},
 * {@link SyncLiteStore#updateUnlogged}, {@link SyncLiteStore#deleteUnlogged}, and
 * their batch variants.
 *
 * All scenarios run sequentially inside a single test on one persistent device.
 * The stageDir accumulates entries across phases and is never wiped mid-test.
 */
class SQLiteStoreUnloggedAPITest {

    @Test
    void testAllUnloggedAPIs() throws Exception {
        Path testHome       = Path.of(System.getProperty("user.home")).resolve("synclite").resolve("test").resolve("javalogger");
        Path testDbPath     = testHome.resolve("db").resolve("SQLiteStoreUnloggedAPITest").resolve("test.db");
        Path testStageDir   = testHome.resolve("stageDir");
        Path testConfigPath = testDbPath.getParent().resolve("synclite.conf");

        // Clean up from any previous run.
        for (int attempt = 0; attempt < 20 && Files.exists(testDbPath.getParent()); attempt++) {
            try {
                deleteRecursively(testDbPath.getParent());
                break;
            } catch (IOException e) {
                Thread.sleep(200);
            }
        }
        if (Files.exists(testStageDir)) {
            try (var dirs = Files.list(testStageDir)) {
                dirs.filter(p -> p.getFileName().toString().startsWith("synclite-unloggedtest-"))
                    .forEach(p -> { try { deleteRecursively(p); } catch (IOException ignored) {} });
            }
        }
        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);
        Files.writeString(testConfigPath,
                "local-data-stage-directory = " + testStageDir + "\ndevice-stage-type = FS\n");

        Class.forName("io.synclite.SQLiteStore");

        // -------------------------------------------------------------------------
        // Phase 0: baseline create "players" table and insert one logged row.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            LinkedHashMap<String, String> cols = new LinkedHashMap<>();
            cols.put("id",    "INTEGER PRIMARY KEY");
            cols.put("name",  "TEXT");
            cols.put("score", "INTEGER");
            store.createTable("unlogged_players", cols);
            store.insert("unlogged_players", Map.of("id", 1, "name", "Alice", "score", 100));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        long baselineCount = countAllCommandlogEntries(testStageDir);
        assertTrue(baselineCount > 0, "Baseline must produce at least one commandlog entry");

        // -------------------------------------------------------------------------
        // Phase 1: openUnlogged writes go to DB only, commandlog unchanged.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.openUnlogged(testDbPath)) {
            store.insert("unlogged_players", Map.of("id", 2, "name", "Bob",   "score", 200));
            store.insert("unlogged_players", Map.of("id", 3, "name", "Carol", "score", 300));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(baselineCount, countAllCommandlogEntries(testStageDir),
                "openUnlogged must not produce new commandlog entries");
        assertEquals(3, rawRowCount(testDbPath, "unlogged_players"),
                "All three rows (1 logged + 2 unlogged) must be present in the device DB");

        // -------------------------------------------------------------------------
        // Phase 2: insertUnlogged single unlogged insert.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        long countBefore = countAllCommandlogEntries(testStageDir);
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insertUnlogged("unlogged_players", Map.of("id", 10, "name", "Unlogged", "score", 999));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countBefore, countAllCommandlogEntries(testStageDir),
                "insertUnlogged must not produce commandlog entries");
        assertEquals(4, rawRowCount(testDbPath, "unlogged_players"));
        assertEquals("Unlogged", rawScalar(testDbPath, "SELECT name FROM unlogged_players WHERE id=10"));

        // -------------------------------------------------------------------------
        // Phase 3: insertBatchUnlogged batch of 3 unlogged inserts.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        countBefore = countAllCommandlogEntries(testStageDir);
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insertBatchUnlogged("unlogged_players", List.of(
                    Map.of("id", 20, "name", "X", "score", 1),
                    Map.of("id", 21, "name", "Y", "score", 2),
                    Map.of("id", 22, "name", "Z", "score", 3)
            ));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countBefore, countAllCommandlogEntries(testStageDir),
                "insertBatchUnlogged must not produce commandlog entries");
        assertEquals(7, rawRowCount(testDbPath, "unlogged_players"));

        // -------------------------------------------------------------------------
        // Phase 4: updateUnlogged update one row without a commandlog entry.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        countBefore = countAllCommandlogEntries(testStageDir);
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.updateUnlogged("unlogged_players", Map.of("score", 9999), Map.of("id", 1));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countBefore, countAllCommandlogEntries(testStageDir),
                "updateUnlogged must not produce commandlog entries");
        assertEquals("9999", rawScalar(testDbPath, "SELECT score FROM unlogged_players WHERE id=1"));

        // -------------------------------------------------------------------------
        // Phase 5: updateBatchUnlogged logged inserts then batch unlogged update.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insert("unlogged_players", Map.of("id", 30, "name", "P", "score", 10));
            store.insert("unlogged_players", Map.of("id", 31, "name", "Q", "score", 20));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
        long countAfterLoggedInserts = countAllCommandlogEntries(testStageDir);
        assertTrue(countAfterLoggedInserts > countBefore, "Logged inserts must produce entries");

        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.updateBatchUnlogged("unlogged_players",
                    List.of(Map.of("score", 100), Map.of("score", 200)),
                    List.of(Map.of("id", 30),     Map.of("id", 31)));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countAfterLoggedInserts, countAllCommandlogEntries(testStageDir),
                "updateBatchUnlogged must not produce commandlog entries");
        assertEquals("100", rawScalar(testDbPath, "SELECT score FROM unlogged_players WHERE id=30"));
        assertEquals("200", rawScalar(testDbPath, "SELECT score FROM unlogged_players WHERE id=31"));

        // -------------------------------------------------------------------------
        // Phase 6: deleteUnlogged delete one row without a commandlog entry.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        countBefore = countAllCommandlogEntries(testStageDir);
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.deleteUnlogged("unlogged_players", Map.of("id", 10)); // remove id=10 (Unlogged)
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countBefore, countAllCommandlogEntries(testStageDir),
                "deleteUnlogged must not produce commandlog entries");
        assertEquals(8, rawRowCount(testDbPath, "unlogged_players")); // 9 rows - 1 deleted

        // -------------------------------------------------------------------------
        // Phase 7: deleteBatchUnlogged logged inserts then batch unlogged delete.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insert("unlogged_players", Map.of("id", 40, "name", "D1", "score", 1));
            store.insert("unlogged_players", Map.of("id", 41, "name", "D2", "score", 2));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
        long countAfterBatchSetup = countAllCommandlogEntries(testStageDir);
        assertEquals(10, rawRowCount(testDbPath, "unlogged_players"));

        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.deleteBatchUnlogged("unlogged_players",
                    List.of(Map.of("id", 40), Map.of("id", 41)));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countAfterBatchSetup, countAllCommandlogEntries(testStageDir),
                "deleteBatchUnlogged must not produce commandlog entries");
        assertEquals(8, rawRowCount(testDbPath, "unlogged_players"));

        // -------------------------------------------------------------------------
        // Phase 8: setTableLogging(false) DML on disabled table skips commandlog.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            LinkedHashMap<String, String> eventCols = new LinkedHashMap<>();
            eventCols.put("id",  "INTEGER PRIMARY KEY");
            eventCols.put("msg", "TEXT");
            store.createTable("unlogged_events", eventCols);
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
        long countAfterDDL = countAllCommandlogEntries(testStageDir);
        assertTrue(countAfterDDL > countAfterBatchSetup, "CREATE TABLE must produce commandlog entries");

        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.setTableLogging("unlogged_players", false);
            store.insert("unlogged_players", Map.of("id", 50, "name", "Shadow", "score", 0)); // unlogged
            store.insert("unlogged_events",  Map.of("id", 1,  "msg", "visible"));             // logged
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        long countAfterMixed = countAllCommandlogEntries(testStageDir);
        assertTrue(countAfterMixed > countAfterDDL,
                "Logged insert to 'events' must produce commandlog entries");
        assertEquals(9, rawRowCount(testDbPath, "unlogged_players")); // 8 + 1 unlogged
        assertEquals(1, rawRowCount(testDbPath, "unlogged_events"));

        // Re-enable logging for "players" by opening a fresh store (state is per-instance).
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insert("unlogged_players", Map.of("id", 51, "name", "Logged", "score", 1));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        long countAfterReEnabled = countAllCommandlogEntries(testStageDir);
        assertTrue(countAfterReEnabled > countAfterMixed,
                "Logged insert after re-enabling must produce commandlog entries");
        assertEquals(10, rawRowCount(testDbPath, "unlogged_players"));

        // -------------------------------------------------------------------------
        // Phase 9: mixed logged + unlogged on the same store instance.
        // -------------------------------------------------------------------------
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        countBefore = countAllCommandlogEntries(testStageDir);
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insert("unlogged_players",         Map.of("id", 60, "name", "Logged1",   "score", 1));
            store.insertUnlogged("unlogged_players", Map.of("id", 61, "name", "Unlogged1", "score", 2));
            store.insert("unlogged_players",         Map.of("id", 62, "name", "Logged2",   "score", 3));
            store.updateUnlogged("unlogged_players", Map.of("score", 9999), Map.of("id", 61));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertTrue(countAllCommandlogEntries(testStageDir) > countBefore,
                "Logged inserts must advance the commandlog");
        assertEquals(13, rawRowCount(testDbPath, "unlogged_players")); // 10 + 3 new
        assertEquals("9999", rawScalar(testDbPath, "SELECT score FROM unlogged_players WHERE id=61"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private long countAllCommandlogEntries(Path stageDir) throws IOException {
        long total = 0;
        if (!Files.exists(stageDir)) return 0;
        Pattern pattern = Pattern.compile("^\\d+\\.sqllog$");
        try (var stream = Files.walk(stageDir)) {
            for (Path p : stream.collect(Collectors.toList())) {
                if (!Files.isRegularFile(p)) continue;
                if (!pattern.matcher(p.getFileName().toString()).matches()) continue;
                try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + p);
                     Statement s = c.createStatement();
                     ResultSet r = s.executeQuery("SELECT COUNT(*) FROM commandlog")) {
                    if (r.next()) total += r.getLong(1);
                } catch (SQLException ignored) {
                    // Not-yet-initialized or malformed segment Ã¢– skip.
                }
            }
        }
        return total;
    }

    private int rawRowCount(Path dbPath, String table) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement s = c.createStatement();
             ResultSet r = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(r.next());
            return r.getInt(1);
        }
    }

    private String rawScalar(Path dbPath, String sql) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement s = c.createStatement();
             ResultSet r = s.executeQuery(sql)) {
            assertTrue(r.next(), "Query returned no rows: " + sql);
            return r.getString(1);
        }
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
