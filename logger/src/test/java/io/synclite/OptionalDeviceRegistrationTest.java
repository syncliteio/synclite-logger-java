package io.synclite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

class OptionalDeviceRegistrationTest {

    @Test
    void sqliteFamilyDevicesStillInitializeWhenDuckDBDriverIsAbsent() throws Exception {
        assertTrue(SyncLite.isDriverAvailable("org.sqlite.JDBC"));
        assertFalse(SyncLite.isDriverAvailable("com.example.DoesNotExist"));

        Path tempDir = Files.createTempDirectory("synclite-optional-driver-test");
        Path databasePath = Files.createTempFile(tempDir, "device", ".db");
        try {
            assertDoesNotThrow(() -> SQLiteStore.initialize(databasePath));
            assertDoesNotThrow(() -> Streaming.initialize(databasePath));
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private static void deleteRecursively(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try {
            Files.walk(path)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (java.io.IOException ignore) {
                            // Best effort cleanup for temporary test data.
                        }
                    });
        } catch (java.io.IOException ignore) {
            // Best effort cleanup for temporary test data.
        }
    }
}
