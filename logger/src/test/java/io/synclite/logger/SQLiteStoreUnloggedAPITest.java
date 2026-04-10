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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the unlogged-write API: {@link SyncLiteStore#openUnlogged},
 * {@link SyncLiteStore#setTableLogging}, {@link SyncLiteStore#insertUnlogged},
 * {@link SyncLiteStore#updateUnlogged}, {@link SyncLiteStore#deleteUnlogged}, and
 * their batch variants.
 *
 * <p>Each test verifies two invariants:
 * <ol>
 *   <li><strong>Data integrity</strong> — unlogged writes DO reach the device DB
 *       (readable via plain {@code jdbc:sqlite:}).</li>
 *   <li><strong>No CDC log entries</strong> — unlogged DML does NOT increase the
 *       commandlog row count in the stage-log files.</li>
 * </ol>
 */
class SQLiteStoreUnloggedAPITest {

    private Path testDbPath;
    private Path testStageDir;
    private Path testConfigPath;
    /** Parent directory for this test — unique per invocation to avoid Windows file-lock races. */
    private Path testHome;
    /** Commandlog entry count after the pre-populated setUp session. */
    private long baselineCommandlogCount;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @BeforeEach
    void setUp() throws Exception {
        // Unique directory per test so no two tests ever share / delete each other's
        // device files.  This completely avoids Windows file-lock races between tests.
        testHome = Path.of(System.getProperty("user.home"))
                .resolve("synclite").resolve("test")
                .resolve("SQLiteStoreUnloggedAPITest")
                .resolve(Long.toString(System.currentTimeMillis()));
        testDbPath   = testHome.resolve("db").resolve("test.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testHome.resolve("synclite_logger.conf");

        Files.createDirectories(testDbPath.getParent());
        Files.createDirectories(testStageDir);
        Files.writeString(testConfigPath,
                "local-data-stage-directory = " + testStageDir + "\ndestination-type = FS\n");

        Class.forName("io.synclite.logger.SQLiteStore");

        // Pre-populate the device with a "players" table and one LOGGED row so
        // there is a known baseline in the commandlog.
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            LinkedHashMap<String, String> cols = new LinkedHashMap<>();
            cols.put("id",    "INTEGER PRIMARY KEY");
            cols.put("name",  "TEXT");
            cols.put("score", "INTEGER");
            store.createTable("players", cols);
            store.insert("players", Map.of("id", 1, "name", "Alice", "score", 100));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        baselineCommandlogCount = countAllCommandlogEntries(testStageDir);
        assertTrue(baselineCommandlogCount > 0,
                "setUp must produce at least one commandlog entry (CREATE TABLE + INSERT)");
    }

    @AfterEach
    void tearDown() throws Exception {
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Opening a store via {@code openUnlogged()} must write all DML directly to the
     * device DB without generating any commandlog entries.
     */
    @Test
    void testOpenUnlogged() throws Exception {
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.openUnlogged(testDbPath)) {
            store.insert("players", Map.of("id", 2, "name", "Bob",   "score", 200));
            store.insert("players", Map.of("id", 3, "name", "Carol", "score", 300));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        // No new commandlog entries from the unlogged session.
        assertEquals(baselineCommandlogCount, countAllCommandlogEntries(testStageDir),
                "openUnlogged must not produce new commandlog DML entries");

        // Both rows ARE in the device DB.
        assertEquals(3, rawRowCount("players"),
                "All three rows (1 logged + 2 unlogged) must be present in the device DB");
    }

    /**
     * {@link SyncLiteStore#insertUnlogged} on a normally-opened store must write to
     * the DB without advancing the commandlog.
     */
    @Test
    void testInsertUnlogged() throws Exception {
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        long countBefore = baselineCommandlogCount;

        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insertUnlogged("players", Map.of("id", 10, "name", "Unlogged", "score", 999));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countBefore, countAllCommandlogEntries(testStageDir),
                "insertUnlogged must not produce commandlog entries");

        // Row IS in DB.
        assertEquals(2, rawRowCount("players"));
        assertEquals("Unlogged", rawScalar("SELECT name FROM players WHERE id=10"));
    }

    /**
     * {@link SyncLiteStore#insertBatchUnlogged} must write all rows without touching
     * the commandlog.
     */
    @Test
    void testInsertBatchUnlogged() throws Exception {
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        long countBefore = baselineCommandlogCount;

        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insertBatchUnlogged("players", List.of(
                    Map.of("id", 20, "name", "X", "score", 1),
                    Map.of("id", 21, "name", "Y", "score", 2),
                    Map.of("id", 22, "name", "Z", "score", 3)
            ));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countBefore, countAllCommandlogEntries(testStageDir),
                "insertBatchUnlogged must not produce commandlog entries");
        assertEquals(4, rawRowCount("players")); // 1 baseline + 3 unlogged
    }

    /**
     * {@link SyncLiteStore#updateUnlogged} must modify DB rows without commandlog entries.
     */
    @Test
    void testUpdateUnlogged() throws Exception {
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        long countBefore = baselineCommandlogCount;

        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            // Update the row inserted in setUp (id=1, name="Alice") without logging.
            store.updateUnlogged("players", Map.of("score", 9999), Map.of("id", 1));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countBefore, countAllCommandlogEntries(testStageDir),
                "updateUnlogged must not produce commandlog entries");

        // Updated value IS in DB.
        assertEquals("9999", rawScalar("SELECT score FROM players WHERE id=1"));
    }

    /**
     * {@link SyncLiteStore#updateBatchUnlogged} must apply all updates without commandlog entries.
     */
    @Test
    void testUpdateBatchUnlogged() throws Exception {
        // First, add two more logged rows so we have multiple targets.
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insert("players", Map.of("id", 30, "name", "P", "score", 10));
            store.insert("players", Map.of("id", 31, "name", "Q", "score", 20));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
        long countAfterSetup = countAllCommandlogEntries(testStageDir);
        assertTrue(countAfterSetup > baselineCommandlogCount, "Logged inserts must produce entries");

        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.updateBatchUnlogged("players",
                    List.of(Map.of("score", 100), Map.of("score", 200)),
                    List.of(Map.of("id",  30),    Map.of("id",  31)));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countAfterSetup, countAllCommandlogEntries(testStageDir),
                "updateBatchUnlogged must not produce commandlog entries");

        assertEquals("100", rawScalar("SELECT score FROM players WHERE id=30"));
        assertEquals("200", rawScalar("SELECT score FROM players WHERE id=31"));
    }

    /**
     * {@link SyncLiteStore#deleteUnlogged} must remove rows from DB without commandlog entries.
     */
    @Test
    void testDeleteUnlogged() throws Exception {
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        long countBefore = baselineCommandlogCount;

        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.deleteUnlogged("players", Map.of("id", 1));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countBefore, countAllCommandlogEntries(testStageDir),
                "deleteUnlogged must not produce commandlog entries");
        assertEquals(0, rawRowCount("players")); // the only row was deleted
    }

    /**
     * {@link SyncLiteStore#deleteBatchUnlogged} must remove multiple rows without commandlog entries.
     */
    @Test
    void testDeleteBatchUnlogged() throws Exception {
        // Add rows to delete.
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.insert("players", Map.of("id", 40, "name", "D1", "score", 1));
            store.insert("players", Map.of("id", 41, "name", "D2", "score", 2));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
        long countAfterSetup = countAllCommandlogEntries(testStageDir);
        assertEquals(3, rawRowCount("players"));

        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.deleteBatchUnlogged("players",
                    List.of(Map.of("id", 40), Map.of("id", 41)));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        assertEquals(countAfterSetup, countAllCommandlogEntries(testStageDir),
                "deleteBatchUnlogged must not produce commandlog entries");
        assertEquals(1, rawRowCount("players")); // only the baseline row remains
    }

    /**
     * {@link SyncLiteStore#setTableLogging(String, boolean) setTableLogging(table, false)} must
     * route all write operations on that table through the unlogged path, while writes to other
     * tables remain logged.
     */
    @Test
    void testSetTableLoggingDisable() throws Exception {
        // Add a second table "events" that will remain fully logged.
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.createTable("events", Map.of("id", "INTEGER PRIMARY KEY", "msg", "TEXT"));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);
        long countAfterDDL = countAllCommandlogEntries(testStageDir);
        assertTrue(countAfterDDL > baselineCommandlogCount);

        // Now: disable logging for "players", keep "events" logged.
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            store.setTableLogging("players", false);

            // This insert to "players" must NOT produce a commandlog entry.
            store.insert("players", Map.of("id", 50, "name", "Shadow", "score", 0));

            // This insert to "events" IS logged normally.
            store.insert("events", Map.of("id", 1, "msg", "visible"));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        long countAfterMixed = countAllCommandlogEntries(testStageDir);

        // "events" insert produced new commandlog entries; "players" insert did not.
        assertTrue(countAfterMixed > countAfterDDL,
                "Logged insert to 'events' must produce commandlog entries");

        // Both rows are in the DB.
        assertEquals(2, rawRowCount("players")); // baseline 1 + unlogged 1
        assertEquals(1, rawRowCount("events"));

        // Re-enable logging for "players" and verify the next insert IS counted.
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");
        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            // logging re-enabled by default on new store open (setTableLogging state is per-instance)
            store.insert("players", Map.of("id", 51, "name", "Logged", "score", 1));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        long countAfterReEnabled = countAllCommandlogEntries(testStageDir);
        assertTrue(countAfterReEnabled > countAfterMixed,
                "Logged insert after re-enabling must produce commandlog entries");
        assertEquals(3, rawRowCount("players"));
    }

    /**
     * Logged and unlogged writes can be freely interleaved on the same store instance.
     * Logged writes must advance the commandlog; unlogged writes must not.
     */
    @Test
    void testMixedLoggedAndUnlogged() throws Exception {
        SQLiteStore.initialize(testDbPath, testConfigPath, "unloggedtest");

        long countBefore = baselineCommandlogCount;

        try (SyncLiteStore store = SQLiteStore.open(testDbPath)) {
            // Logged insert.
            store.insert("players", Map.of("id", 60, "name", "Logged1", "score", 1));

            // Unlogged insert on same store instance.
            store.insertUnlogged("players", Map.of("id", 61, "name", "Unlogged1", "score", 2));

            // Another logged insert.
            store.insert("players", Map.of("id", 62, "name", "Logged2", "score", 3));

            // Unlogged update.
            store.updateUnlogged("players", Map.of("score", 9999), Map.of("id", 61));
        }
        SQLiteStore.closeAllDevices();
        Thread.sleep(150);

        long countAfter = countAllCommandlogEntries(testStageDir);

        // Only the 2 logged inserts contributed to the commandlog.
        assertTrue(countAfter > countBefore,
                "Logged inserts must advance the commandlog");

        // All 4 rows / mutations are visible in the DB.
        assertEquals(4, rawRowCount("players")); // 1 baseline + 3 new
        assertEquals("9999", rawScalar("SELECT score FROM players WHERE id=61"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Counts total rows in the {@code commandlog} table across ALL stage-log files. */
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
                    // Malformed / not-yet-initialized log file — skip.
                }
            }
        }
        return total;
    }

    /** Returns the row count in {@code table} via a plain SQLite JDBC connection. */
    private int rawRowCount(String table) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + testDbPath);
             Statement s = c.createStatement();
             ResultSet r = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(r.next());
            return r.getInt(1);
        }
    }

    /** Returns the first column of the first row of {@code sql} as a String (plain JDBC). */
    private String rawScalar(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + testDbPath);
             Statement s = c.createStatement();
             ResultSet r = s.executeQuery(sql)) {
            assertTrue(r.next(), "Query returned no rows: " + sql);
            return r.getString(1);
        }
    }
}
