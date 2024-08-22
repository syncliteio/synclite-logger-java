package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashMap;

public abstract class DBProcessor {

	protected static final String createTxnTableIfNotExistsSql = "CREATE TABLE IF NOT EXISTS synclite_txn(commit_id BIGINT PRIMARY KEY, operation_id BIGINT)";
	protected static final String createTxnTableSql = "CREATE TABLE synclite_txn(commit_id BIGINT PRIMARY KEY, operation_id BIGINT)";
	protected static final String dropTxnTableSql = "DROP TABLE IF EXISTS synclite_txn";
	protected static final String selectTxnTableSql = "SELECT commit_id, operation_id FROM synclite_txn";
	protected static final String insertTxnTable = "INSERT INTO synclite_txn VALUES(0,0)";

	public abstract HashMap<String, Long> initReadCommitID(Path dbPath, long pageSize) throws SQLException;
	public abstract void backupDB(Path srcDB, Path dstDB, SyncLiteOptions options, boolean schemaOnly) throws SQLException;
	public abstract void processBackupDB(Path backupDB, SyncLiteOptions options) throws SQLException;

	public static DBProcessor getInstance(DeviceType deviceType) throws SQLException {
		switch (deviceType) {
		case SQLITE:
		case TELEMETRY:
		case SQLITE_APPENDER:
			return new SQLiteProcessor();
		case STREAMING:
			return new StreamingProcessor();
		case DUCKDB:
		case DUCKDB_APPENDER:	
			return new DuckDBProcessor();
		case DERBY:
		case DERBY_APPENDER:	
			return new DerbyProcessor();
		case H2:
		case H2_APPENDER:	
			return new H2Processor();
		case HYPERSQL:
		case HYPERSQL_APPENDER:	
			return new HyperSQLProcessor();
		}
		throw new SQLException("Unsupported device type : " + deviceType);
	}
}
