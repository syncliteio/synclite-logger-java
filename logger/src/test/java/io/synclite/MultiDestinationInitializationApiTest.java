package io.synclite;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MultiDestinationInitializationApiTest {

    @TempDir
    Path tempDir;

    @Test
    void rejectsInvalidDestinationListsBeforeCreatingDeviceState() {
        Path dbPath = tempDir.resolve("invalid.sqlite");

        SQLException empty = assertThrows(SQLException.class,
                () -> SyncLite.initialize(
                        DeviceType.SQLITE, dbPath, List.of()));
        assertTrue(empty.getMessage().contains("at least one"));

        SQLException nullEntry = assertThrows(SQLException.class,
                () -> SyncLite.initialize(
                        DeviceType.SQLITE, dbPath,
                        Arrays.asList((DestinationOptions) null)));
        assertTrue(nullEntry.getMessage().contains("destination 1"));

        DestinationOptions emptyConnection = DestinationOptions.builder()
                .dstType(DstType.SQLITE)
                .connectionString("   ")
                .build();
        SQLException emptyConnectionError = assertThrows(SQLException.class,
                () -> SyncLite.initialize(
                        DeviceType.SQLITE, dbPath, List.of(emptyConnection)));
        assertTrue(emptyConnectionError.getMessage().contains("destination 1"));
        assertFalse(Files.exists(Path.of(dbPath.toString() + ".synclite")));
    }

    @Test
    void allDeviceFacadesExposeSingleAndMultiDestinationApis() throws Exception {
        List<Class<?>> facades = List.of(
                DBLogger.class,
                Derby.class,
                DerbyAppender.class,
                DerbyStore.class,
                DuckDB.class,
                DuckDBAppender.class,
                DuckDBStore.class,
                H2.class,
                H2Appender.class,
                H2Store.class,
                HyperSQL.class,
                HyperSQLAppender.class,
                HyperSQLStore.class,
                SQLite.class,
                SQLiteAppender.class,
                SQLiteStore.class,
                Streaming.class);

        for (Class<?> facade : facades) {
            assertNotNull(facade.getMethod(
                    "initialize", Path.class, DestinationOptions.class));
            assertNotNull(facade.getMethod(
                    "initialize", Path.class, List.class));
            assertNotNull(facade.getMethod(
                    "initialize",
                    Path.class, String.class, List.class));
            assertNotNull(facade.getMethod(
                    "initialize",
                    Path.class, SyncLiteOptions.class, List.class));
            assertNotNull(facade.getMethod(
                    "initialize",
                    Path.class, SyncLiteOptions.class, String.class, List.class));
            assertNotNull(facade.getMethod(
                    "initialize",
                    Path.class, Path.class, List.class));
            assertNotNull(facade.getMethod(
                    "initialize",
                    Path.class, Path.class, String.class, List.class));
        }
    }
}
