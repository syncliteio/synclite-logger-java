/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.synclite;

import java.nio.file.Path;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Consolidator-module mirror of {@code io.synclite.DuckDBStoreAPITest}.
 * Disabled when {@code -DskipJunitDataConsolidation=true} is passed.
 */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class DuckDBStoreAPITestWithConsolidator {

    private static final String TEST_NAME   = "DuckDBStoreAPITestWithConsolidator";
    private static final String DEVICE_NAME = "duckdbstoreapi";

    private Path testDbPath;
    private Path testConfigPath;
    private DestinationOptions destination;

    @BeforeEach
    void setUp() throws Exception {
        ConsolidationTestSupport.resetTestDirs(TEST_NAME, DEVICE_NAME);
        testDbPath     = ConsolidationTestSupport.testRoot(TEST_NAME).resolve("test.db");
        testConfigPath = ConsolidationTestSupport.writeConfig(TEST_NAME);
        destination    = ConsolidationTestSupport.sqliteDestination(TEST_NAME);

        Class.forName("io.synclite.DuckDBStore");
        io.synclite.DuckDBStore.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            io.synclite.DuckDBStore.closeDevice(testDbPath);
        } catch (java.sql.SQLException ignored) {
        }
        Thread.sleep(150);
    }

    @Test
    void testAllAPIs() throws Exception {
        try (SyncLiteStore store = DuckDBStore.open(testDbPath)) {
            SQLiteStoreAPITest.runAPITest(store, "duckdbstoreapi_players");
        }
        ConsolidationTestSupport.awaitAndAssertDestinationTableExists(
                TEST_NAME, testDbPath, "duckdbstoreapi_players", java.time.Duration.ofSeconds(30));
    }
}
