import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import io.synclite.logger.SQLiteAppender;

/**
 * Appender device sample.
 *
 * Appender devices are meant for write-heavy ingest paths where the workload is
 * dominated by appends. They keep the calling pattern simple and emphasize
 * durable INSERT-style logging rather than broad relational mutation.
 *
 * Compared to a SQL device, this is intentionally narrower. If the workload
 * needs arbitrary SQL semantics and statement replay on a replica, use a SQL
 * device instead.
 */
public class SyncLiteAppenderDeviceApp {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        appStartup();
        SyncLiteAppenderDeviceApp app = new SyncLiteAppenderDeviceApp();
        app.runBusinessLogic();
    }

    public static void appStartup() throws SQLException, ClassNotFoundException {
        // Appender default: SQLiteAppender.
        // Replace for other appender engines:
        // 1) Driver class: io.synclite.logger.SQLiteAppender -> DerbyAppender, DuckDBAppender, H2Appender, HyperSQLAppender
        // 2) Initialize call: SQLiteAppender.initialize(...) -> <Engine>Appender.initialize(...)
        // 3) JDBC URL prefix in runBusinessLogic():
        //    jdbc:synclite_sqlite_appender: -> jdbc:synclite_derby_appender:, jdbc:synclite_duckdb_appender:, jdbc:synclite_h2_appender:, jdbc:synclite_hsqldb_appender:
        Class.forName("io.synclite.logger.SQLiteAppender");
        Path dbPath = Path.of("sample_appender_sqlite.db");
        SQLiteAppender.initialize(dbPath, Path.of("synclite_logger.conf"));
    }

    public void runBusinessLogic() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:synclite_sqlite_appender:sample_appender_sqlite.db")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS feedback(rating INT, comment TEXT)");
            }

            // Appender devices support DDL, INSERT, and SELECT.
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO feedback VALUES(?, ?)")) {
                pstmt.setInt(1, 4);
                pstmt.setString(2, "Excellent Product");
                pstmt.addBatch();

                pstmt.setInt(1, 5);
                pstmt.setString(2, "Outstanding Product");
                pstmt.addBatch();

                pstmt.executeBatch();
            }
        }

        SQLiteAppender.closeDevice(Path.of("sample_appender_sqlite.db"));
    }
}
