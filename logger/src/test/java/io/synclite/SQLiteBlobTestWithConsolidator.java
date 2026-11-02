package io.synclite;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/** Real Java SQLite logger -> JNI Rust embedded worker -> SQLite regression. */
@DisabledIfSystemProperty(named = "skipJunitDataConsolidation", matches = "true")
class SQLiteBlobTestWithConsolidator {
    @TempDir
    Path root;

    @Test
    void statementAndPreparedStatementPreserveBlobBytes() throws Exception {
        assertBlobRoundTrip(DstSyncMode.CONSOLIDATION);
    }

    @Test
    void statementAndPreparedStatementPreserveBlobBytesInReplicationMode() throws Exception {
        assertBlobRoundTrip(DstSyncMode.REPLICATION);
    }

    private void assertBlobRoundTrip(DstSyncMode syncMode) throws Exception {
        String destinationUrl = "jdbc:sqlite:" + normalized(root.resolve("destination.sqlite"));
        Path source = root.resolve("source.sqlite");
        Path config = root.resolve("synclite.conf");
        Files.writeString(config,
                "device-stage-type = FS\n"
                + "local-data-stage-directory = " + normalized(root.resolve("stage")) + "\n"
                + "device-data-root = " + normalized(root.resolve("work")) + "\n"
                + "log-segment-switch-duration-threshold-ms = 100\n");
        byte[] payload = {0, 1, 39, 63, 92, 127, (byte) 128, (byte) 255};

        try (Connection target = DriverManager.getConnection(destinationUrl)) {
            try {
                DestinationOptions destination = DestinationOptions.builder()
                        .dstType(DstType.SQLITE).connectionString(destinationUrl)
                    .syncMode(syncMode).build();
                SQLite.initialize(source, config, "blobregression", destination);
                try (Connection local = DriverManager.getConnection("jdbc:synclite_sqlite:" + source)) {
                    try (Statement statement = local.createStatement()) {
                        statement.execute("CREATE TABLE blob_items (id INTEGER PRIMARY KEY, payload BLOB)");
                        statement.executeUpdate("INSERT INTO blob_items VALUES (1, X'0001273f5c7f80ff')");
                    }
                    SyncLite.awaitSync(source, Duration.ofSeconds(30));
                    assertPayload(target, 1, payload);
                    System.out.println("BLOB literal Statement replicated correctly through Rust JNI worker");

                    try (PreparedStatement statement = local.prepareStatement(
                            "INSERT INTO blob_items (id, payload) VALUES (?, ?)")) {
                        statement.setInt(1, 2);
                        statement.setBytes(2, payload);
                        assertEquals(1, statement.executeUpdate());
                    }
                    assertPayload(local, 2, payload);
                    SyncLite.awaitSync(source, Duration.ofSeconds(30));
                    assertPayload(target, 2, payload);
                    System.out.println("BLOB PreparedStatement.setBytes replicated correctly through Rust JNI worker");

                    try (PreparedStatement statement = local.prepareStatement(
                            "INSERT INTO blob_items (id, payload) VALUES (?, ?)")) {
                        statement.setInt(1, 3);
                        statement.setBytes(2, new byte[0]);
                        statement.addBatch();
                        statement.setInt(1, 4);
                        statement.setNull(2, Types.BLOB);
                        statement.addBatch();
                        statement.setInt(1, 5);
                        statement.setBytes(2, payload);
                        statement.addBatch();
                        assertEquals(3, statement.executeBatch().length);
                    }
                    SyncLite.awaitSync(source, Duration.ofSeconds(30));
                    assertPayload(target, 3, new byte[0]);
                    assertPayload(target, 4, null);
                    assertPayload(target, 5, payload);

                    byte[] large = new byte[65536];
                    for (int i = 0; i < large.length; i++) {
                        large[i] = (byte) i;
                    }
                    try (PreparedStatement statement = local.prepareStatement(
                            "INSERT INTO blob_items (id, payload) VALUES (?, ?)")) {
                        statement.setInt(1, 6);
                        statement.setObject(2, large);
                        assertEquals(1, statement.executeUpdate());
                        statement.setInt(1, 7);
                        statement.setBinaryStream(2, new ByteArrayInputStream(payload), payload.length);
                        assertEquals(1, statement.executeUpdate());
                    }
                    SyncLite.awaitSync(source, Duration.ofSeconds(30));
                    assertPayload(target, 6, large);
                    assertPayload(target, 7, payload);
                }
            } finally {
                SQLite.closeDevice(source);
            }
        }
    }

    private static void assertPayload(Connection connection, int id, byte[] expected) throws Exception {
        try (PreparedStatement query = connection.prepareStatement(
            "SELECT payload, typeof(payload) FROM blob_items WHERE id = ?")) {
            query.setInt(1, id);
            try (ResultSet rows = query.executeQuery()) {
                assertTrue(rows.next(), "missing row " + id);
                assertArrayEquals(expected, rows.getBytes(1), "BLOB mismatch for row " + id);
                assertEquals(expected == null ? "null" : "blob", rows.getString(2),
                    "storage class mismatch for row " + id);
                assertFalse(rows.next(), "duplicate row " + id);
            }
        }
    }

    private static String normalized(Path path) {
        return path.toAbsolutePath().toString().replace('\\', '/');
    }
}