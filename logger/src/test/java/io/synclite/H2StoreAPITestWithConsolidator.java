/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.synclite;

import java.nio.file.Path;
import java.sql.SQLException;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Consolidator-module mirror of {@code io.synclite.H2StoreAPITest}.
 * Disabled when {@code -DskipJunitDataConsolidation=true} is passed.
 */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class H2StoreAPITestWithConsolidator {

    private static final String TEST_NAME   = "H2StoreAPITestWithConsolidator";
    private static final String DEVICE_NAME = "h2storeapi";

    private Path testDbPath;
    private Path testConfigPath;
    private DestinationOptions destination;

    @BeforeEach
    void setUp() throws Exception {
        ConsolidationTestSupport.resetTestDirs(TEST_NAME, DEVICE_NAME);
        testDbPath     = ConsolidationTestSupport.testRoot(TEST_NAME).resolve("test.db");
        testConfigPath = ConsolidationTestSupport.writeConfig(TEST_NAME);
        destination    = ConsolidationTestSupport.sqliteDestination(TEST_NAME);

        Class.forName("io.synclite.H2Store");
        io.synclite.H2Store.initialize(testDbPath, testConfigPath, DEVICE_NAME, destination);
    }

    @AfterEach
    void tearDown() throws Exception {
        try { io.synclite.H2Store.closeDevice(testDbPath); } catch (SQLException ignored) {}
        Thread.sleep(150);
    }

    @Test
    void testAllAPIs() throws Exception {
        try (SyncLiteStore store = H2Store.open(testDbPath)) {
            SQLiteStoreAPITest.runAPITest(store, "h2storeapi_players");
        }
        ConsolidationTestSupport.awaitAndAssertDestinationTableExists(
                TEST_NAME, testDbPath, "h2storeapi_players", java.time.Duration.ofSeconds(30));
    }
}
