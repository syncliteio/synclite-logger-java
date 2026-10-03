package io.synclite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class MultiDestinationInitializationIntegrationTest {

    @TempDir
    Path tempDir;

    private Path sourceDb;

    @AfterEach
    void tearDown() throws Exception {
        if (sourceDb != null) {
            try {
                SyncLite.closeDevice(sourceDb);
            } catch (SQLException ignored) {
                // The test may already have closed or rolled back the device.
            }
        }
    }

    @Test
    void initializesSequentialDestinationsAndAwaitsAllOfThem() throws Exception {
        sourceDb = tempDir.resolve("source.sqlite");
        Path stageDir = tempDir.resolve("stage");
        Path workDir = tempDir.resolve("work");
        Path firstDb = tempDir.resolve("first.sqlite");
        Path secondDb = tempDir.resolve("second.sqlite");
        Path config = tempDir.resolve("synclite.conf");
        Files.writeString(config,
                "local-data-stage-directory = " + normalized(stageDir) + "\n"
                        + "device-stage-type = FS\n"
                        + "device-data-root = " + normalized(workDir) + "\n");

        DestinationOptions first = sqliteDestination(firstDb);
        DestinationOptions second = sqliteDestination(secondDb);
        SQLite.initialize(
                sourceDb, config, "multiDestinationDevice", List.of(first, second));

        assertTrue(Files.isDirectory(workDir.resolve("DB-1")));
        assertTrue(Files.isDirectory(workDir.resolve("DB-2")));

        waitForTable(secondDb, "synclite_checkpoint", Duration.ofSeconds(10));
        try (Connection blocker = DriverManager.getConnection(
            "jdbc:sqlite:" + normalized(secondDb));
            Statement blockerStatement = blocker.createStatement()) {
            blockerStatement.execute("BEGIN EXCLUSIVE");

            try (Connection connection = DriverManager.getConnection(
                "jdbc:synclite_sqlite:" + sourceDb);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE multi_destination_items "
                + "(id INTEGER PRIMARY KEY, name TEXT)");
            statement.execute("INSERT INTO multi_destination_items VALUES (1, 'one')");
            }

            waitForRowCount(firstDb, "multi_destination_items", 1L, Duration.ofSeconds(10));
            SQLException timeout = assertThrows(SQLException.class,
                () -> SyncLite.awaitSync(sourceDb, Duration.ofMillis(300)));
            assertTrue(timeout.getMessage().contains("destination_checkpoints=["));
                assertTrue(timeout.getMessage().contains("2="));

            blockerStatement.execute("ROLLBACK");
        }

        SyncLite.awaitSync(sourceDb, Duration.ofSeconds(30));
        assertEquals(1L, countRows(firstDb, "multi_destination_items"));
        assertEquals(1L, countRows(secondDb, "multi_destination_items"));
    }

    @Test
    void nativeSpawnErrorsUseTheJavaSyncLiteException() {
        assertThrows(SyncLiteException.class, () ->
                NativeConsolidator.nativeSpawnConsolidator(
                        tempDir.resolve("source.sqlite").toString(),
                        tempDir.resolve("work").toString(),
                        tempDir.resolve("work").toString(),
                        "device-id",
                        "device-name",
                        DeviceType.SQLITE.name(),
                        "source",
                        DstType.SQLITE.name(),
                        tempDir.resolve("destination.sqlite").toString(),
                        DstSyncMode.CONSOLIDATION.name(),
                        null,
                        null,
                        "DESTINATION",
                        tempDir.resolve("stage").toString(),
                        1,
                        0,
                        500L));
    }

    @Test
    void laterDestinationStartupFailureRollsBackEarlierWorkerAndLogger() throws Exception {
        sourceDb = tempDir.resolve("rollback-source.sqlite");
        Path stageDir = tempDir.resolve("rollback-stage");
        Path workDir = tempDir.resolve("rollback-work");
        Path config = tempDir.resolve("rollback.conf");
        Files.createDirectories(workDir);
        Files.writeString(workDir.resolve("DB-2"), "blocks directory creation");
        Files.writeString(config,
                "local-data-stage-directory = " + normalized(stageDir) + "\n"
                        + "device-stage-type = FS\n"
                        + "device-data-root = " + normalized(workDir) + "\n");

        DestinationOptions first = sqliteDestination(tempDir.resolve("rollback-first.sqlite"));
        DestinationOptions second = sqliteDestination(tempDir.resolve("rollback-second.sqlite"));

        assertThrows(SQLException.class, () -> SQLite.initialize(
                sourceDb, config, "rollbackDevice", List.of(first, second)));
        assertNull(SQLLogger.findInstance(sourceDb.toAbsolutePath()));

        // Renaming the first worker's work root proves its SQLite handles were
        // released by rollback on Windows rather than left running detached.
        Path firstWorkDir = workDir.resolve("DB-1");
        Files.move(firstWorkDir, workDir.resolve("DB-1-rolled-back"));

        Files.delete(workDir.resolve("DB-2"));
        SQLite.initialize(sourceDb, config, "rollbackDevice", first);
        assertTrue(SQLLogger.findInstance(sourceDb.toAbsolutePath()) != null);
    }

    private static DestinationOptions sqliteDestination(Path dbPath) {
        return DestinationOptions.builder()
                .dstType(DstType.SQLITE)
                .connectionString("jdbc:sqlite:" + normalized(dbPath))
                .syncMode(DstSyncMode.CONSOLIDATION)
                .build();
    }

    private static long countRows(Path dbPath, String table) throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:sqlite:" + normalized(dbPath));
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(
                        "SELECT COUNT(*) FROM \"" + table + "\"")) {
            rows.next();
            return rows.getLong(1);
        }
    }

    private static void waitForTable(Path dbPath, String table, Duration timeout)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Connection connection = DriverManager.getConnection(
                    "jdbc:sqlite:" + normalized(dbPath));
                    java.sql.PreparedStatement statement = connection.prepareStatement(
                            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")) {
                statement.setString(1, table);
                try (ResultSet rows = statement.executeQuery()) {
                    if (rows.next()) {
                        return;
                    }
                }
            }
            Thread.sleep(25L);
        }
        throw new AssertionError("timed out waiting for table " + table + " in " + dbPath);
    }

    private static void waitForRowCount(
            Path dbPath, String table, long expected, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        SQLException lastError = null;
        while (System.nanoTime() < deadline) {
            try {
                if (countRows(dbPath, table) == expected) {
                    return;
                }
                lastError = null;
            } catch (SQLException error) {
                lastError = error;
            }
            Thread.sleep(25L);
        }
        AssertionError failure = new AssertionError(
                "timed out waiting for " + expected + " rows in " + table + " at " + dbPath);
        if (lastError != null) {
            failure.initCause(lastError);
        }
        throw failure;
    }

    private static String normalized(Path path) {
        return path.toAbsolutePath().toString().replace('\\', '/');
    }
}
