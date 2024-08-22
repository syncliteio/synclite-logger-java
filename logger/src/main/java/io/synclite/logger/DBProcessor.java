/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

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
