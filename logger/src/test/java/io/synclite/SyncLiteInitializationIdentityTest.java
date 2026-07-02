package io.synclite;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SyncLiteInitializationIdentityTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void tearDown() throws Exception {
        SyncLite.closeAllDevices();
        org.apache.log4j.LogManager.shutdown();
        Thread.sleep(500);
    }

    @Test
    void rejectsDeviceNameMismatchOnReinitialize() throws Exception {
        Path dbPath = tempDir.resolve("identity-db.sqlite");

        DBLogger.initialize(dbPath, "deviceA");

        SQLException ex = assertThrows(SQLException.class, () -> DBLogger.initialize(dbPath, "deviceB"));
        assertTrue(ex.getMessage().contains("device name"));
    }

    @Test
    void rejectsDeviceNameMismatchFromConfigFile() throws Exception {
        Path dbPath = tempDir.resolve("config-identity-db.sqlite");
        Path propsPath = tempDir.resolve("synclite.conf");

        Files.writeString(propsPath, "device-name=deviceA\n");
        DBLogger.initialize(dbPath, propsPath);

        Files.writeString(propsPath, "device-name=deviceB\n");

        SQLException ex = assertThrows(SQLException.class, () -> DBLogger.initialize(dbPath, propsPath));
        assertTrue(ex.getMessage().contains("device name"));
    }

    @Test
    void rejectsDeviceTypeMismatchOnReinitialize() throws Exception {
        Path dbPath = tempDir.resolve("type-identity-db.sqlite");

        DBLogger.initialize(dbPath);

        SQLException ex = assertThrows(SQLException.class, () -> SQLite.initialize(dbPath));
        assertTrue(ex.getMessage().contains("device type"));
    }

    @Test
    void rejectsDeviceTypeMismatchFromConfigFile() throws Exception {
        Path dbPath = tempDir.resolve("config-type-identity-db.sqlite");
        Path propsPath = tempDir.resolve("synclite.conf");

        Files.writeString(propsPath, "device-type=dblogger\n");
        DBLogger.initialize(dbPath, propsPath);

        Files.writeString(propsPath, "device-type=sqlite\n");

        SQLException ex = assertThrows(SQLException.class, () -> SQLite.initialize(dbPath, propsPath));
        assertTrue(ex.getMessage().contains("device type"));
    }
}
