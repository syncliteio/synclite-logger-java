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
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
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
        Path testHome = Path.of(System.getProperty("user.home"))
                .resolve("synclite").resolve("test").resolve("SQLiteStoreAPITest");
        testDbPath = testHome.resolve("db").resolve("test.db");
        testStageDir = testHome.resolve("stageDir");
        testConfigPath = testHome.resolve("synclite_logger.conf");

        if (Files.exists(testHome)) deleteRecursively(testHome);
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
            runAPITest(store);
        }
    }

    static void runAPITest(SyncLiteStore store) throws Exception {
        // --- createTable ---
        Map<String, String> cols = new LinkedHashMap<>();
        cols.put("id", "INTEGER PRIMARY KEY");
        cols.put("name", "VARCHAR(255)");
        cols.put("score", "INTEGER");
        store.createTable("players", cols);

        // --- insert single row ---
        store.insert("players", Map.of("id", 1, "name", "Alice", "score", 100));
        store.insert("players", Map.of("id", 2, "name", "Bob", "score", 200));

        // --- selectAll ---
        List<Map<String, Object>> rows = store.selectAll("players");
        assertEquals(2, rows.size());

        // --- select with where ---
        List<Map<String, Object>> aliceRows = store.select("players", Map.of("name", "Alice"));
        assertEquals(1, aliceRows.size());
        assertEquals(100, ((Number) aliceRows.get(0).get("score")).intValue());

        // --- update ---
        store.update("players", Map.of("score", 999), Map.of("name", "Alice"));
        List<Map<String, Object>> updated = store.select("players", Map.of("name", "Alice"));
        assertEquals(999, ((Number) updated.get(0).get("score")).intValue());

        // --- delete ---
        store.delete("players", Map.of("name", "Bob"));
        assertEquals(1, store.selectAll("players").size());

        // --- insertBatch ---
        List<Map<String, Object>> batch = List.of(
                Map.of("id", 3, "name", "Carol", "score", 300),
                Map.of("id", 4, "name", "Dave", "score", 400)
        );
        store.insertBatch("players", batch);
        assertEquals(3, store.selectAll("players").size());

        // --- updateBatch ---
        store.updateBatch("players",
                List.of(Map.of("score", 350), Map.of("score", 450)),
                List.of(Map.of("name", "Carol"), Map.of("name", "Dave")));
        assertEquals(350, ((Number) store.select("players", Map.of("name", "Carol")).get(0).get("score")).intValue());
        assertEquals(450, ((Number) store.select("players", Map.of("name", "Dave")).get(0).get("score")).intValue());

        // --- deleteBatch ---
        store.deleteBatch("players", List.of(Map.of("name", "Carol"), Map.of("name", "Dave")));
        assertEquals(1, store.selectAll("players").size());

        // --- auto column addition on insert ---
        // Insert a row with a brand-new column "level" that doesn't exist yet.
        store.insert("players", Map.of("id", 5, "name", "Eve", "score", 500, "level", 7));
        List<Map<String, Object>> eveRows = store.select("players", Map.of("name", "Eve"));
        assertEquals(1, eveRows.size());
        assertEquals(7, ((Number) eveRows.get(0).get("level")).intValue());

        // --- auto column addition on update ---
        // Update with a new column "badge" that doesn't exist yet.
        store.update("players", Map.of("badge", "gold"), Map.of("name", "Eve"));
        List<Map<String, Object>> eveUpdated = store.select("players", Map.of("name", "Eve"));
        assertEquals("gold", eveUpdated.get(0).get("badge"));

        // --- transactional insert + rollback ---
        store.setAutoCommit(false);
        store.insert("players", Map.of("id", 99, "name", "Temp", "score", 0));
        store.rollback();
        assertTrue(store.select("players", Map.of("name", "Temp")).isEmpty(),
                "Rolled-back row must not be visible");

        // --- transactional insert + commit ---
        store.setAutoCommit(false);
        store.insert("players", Map.of("id", 6, "name", "Frank", "score", 600));
        store.commit();
        assertEquals(1, store.select("players", Map.of("name", "Frank")).size());

        // --- dropTable ---
        store.dropTable("players");
        // Table is gone; a fresh createTable must succeed
        store.createTable("players", Map.of("id", "INTEGER PRIMARY KEY"));
        assertEquals(0, store.selectAll("players").size());
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
