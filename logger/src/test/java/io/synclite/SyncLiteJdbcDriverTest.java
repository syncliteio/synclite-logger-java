package io.synclite;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class SyncLiteJdbcDriverTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "jdbc:synclite_sqlite:test.db",
            "jdbc:synclite_sqlite_appender:test.db",
            "jdbc:synclite_sqlite_store:test.db",
            "jdbc:synclite_duckdb:test.duckdb",
            "jdbc:synclite_duckdb_appender:test.duckdb",
            "jdbc:synclite_duckdb_store:test.duckdb",
            "jdbc:synclite_derby:test",
            "jdbc:synclite_derby_appender:test",
            "jdbc:synclite_derby_store:test",
            "jdbc:synclite_h2:test",
            "jdbc:synclite_h2_appender:test",
            "jdbc:synclite_h2_store:test",
            "jdbc:synclite_hsqldb:test",
            "jdbc:synclite_hsqldb_appender:test",
            "jdbc:synclite_hsqldb_store:test",
            "jdbc:synclite_dblogger:test.db",
            "jdbc:synclite_streaming:test.db"
    })
    void registeredDriverAcceptsEveryRoutableSyncLiteUrl(String url) throws Exception {
        Class.forName("io.synclite.SyncLite");

        Driver driver = DriverManager.getDriver(url);
        if (!(driver instanceof SyncLite)) {
            throw new AssertionError("Expected SyncLite driver for " + url + ", got " + driver);
        }

        assertTrue(((SyncLite) driver).acceptsURL(url));
    }

    @ParameterizedTest
    @MethodSource("concreteDriverUrls")
    void concreteDriverClassesPassConnectionPoolUrlValidation(
            Class<? extends Driver> driverClass,
            String url) throws Exception {
        Object driver = driverClass.getDeclaredConstructor().newInstance();
        if (!(driver instanceof SyncLite)) {
            throw new AssertionError("Expected SyncLite driver instance, got " + driver);
        }

        assertTrue(((SyncLite) driver).acceptsURL(url));
    }

    @Test
    void rejectsNativeAndUnknownJdbcUrls() throws Exception {
        Driver driver = new SyncLite();

        assertFalse(driver.acceptsURL("jdbc:sqlite:test.db"));
        assertFalse(driver.acceptsURL("jdbc:synclite_unknown:test.db"));
        assertFalse(driver.acceptsURL(null));
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void implicitDbLoggerInitializationUsesCanonicalStageDirectory(@TempDir Path tempDir) throws Exception {
        String originalUserHome = System.getProperty("user.home");
        Path testHome = tempDir.resolve("home");
        Path dbPath = tempDir.resolve("hibernate.db").toAbsolutePath();
        Path stageDir = testHome.resolve("synclite").resolve("job1").resolve("stageDir");

        Files.createDirectories(testHome);
        try {
            System.setProperty("user.home", testHome.toString());
            Class.forName("io.synclite.SyncLite");

            try (Connection ignored = DriverManager.getConnection(
                    "jdbc:synclite_dblogger:" + dbPath)) {
                assertTrue(Files.isDirectory(Path.of(dbPath + ".synclite")),
                        "The logger sidecar should be created beside the database");
                assertTrue(Files.isDirectory(stageDir),
                        "JDBC auto-initialization should create the canonical stage directory");
                try (Stream<Path> entries = Files.list(stageDir)) {
                    assertTrue(entries.anyMatch(path -> Files.isDirectory(path)
                                    && path.getFileName().toString().startsWith("synclite-")),
                            "The stage directory should contain the device archive");
                }
            }
        } finally {
            SyncLite.closeDevice(dbPath);
            if (originalUserHome == null) {
                System.clearProperty("user.home");
            } else {
                System.setProperty("user.home", originalUserHome);
            }
        }
    }

    private static Stream<Arguments> concreteDriverUrls() {
        return Stream.of(
                Arguments.of(SQLite.class, "jdbc:synclite_sqlite:test.db"),
                Arguments.of(SQLiteAppender.class, "jdbc:synclite_sqlite_appender:test.db"),
                Arguments.of(SQLiteStore.class, "jdbc:synclite_sqlite_store:test.db"),
                Arguments.of(DuckDB.class, "jdbc:synclite_duckdb:test.duckdb"),
                Arguments.of(DuckDBAppender.class, "jdbc:synclite_duckdb_appender:test.duckdb"),
                Arguments.of(DuckDBStore.class, "jdbc:synclite_duckdb_store:test.duckdb"),
                Arguments.of(Derby.class, "jdbc:synclite_derby:test"),
                Arguments.of(DerbyAppender.class, "jdbc:synclite_derby_appender:test"),
                Arguments.of(DerbyStore.class, "jdbc:synclite_derby_store:test"),
                Arguments.of(H2.class, "jdbc:synclite_h2:test"),
                Arguments.of(H2Appender.class, "jdbc:synclite_h2_appender:test"),
                Arguments.of(H2Store.class, "jdbc:synclite_h2_store:test"),
                Arguments.of(HyperSQL.class, "jdbc:synclite_hsqldb:test"),
                Arguments.of(HyperSQLAppender.class, "jdbc:synclite_hsqldb_appender:test"),
                Arguments.of(HyperSQLStore.class, "jdbc:synclite_hsqldb_store:test"),
                Arguments.of(DBLogger.class, "jdbc:synclite_dblogger:test.db"),
                Arguments.of(Streaming.class, "jdbc:synclite_streaming:test.db"));
    }
}
