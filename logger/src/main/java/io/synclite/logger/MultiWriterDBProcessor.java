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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

abstract class MultiWriterDBProcessor extends DBProcessor {

	class Column {
		Column(String name, String type, String defaultvalue, boolean isNull, boolean isPrimaryKey) {
			this.name = name;
			this.type = type;
			this.defaultValue = defaultvalue;
			this.isNullable = isNull;
			this.isPrimaryKey = isPrimaryKey;
		}
		
		String name;
		String type;
		String defaultValue;
		boolean isNullable;
		boolean isPrimaryKey;
	}
	
	protected static final String selectMaxTxnTableSql = "SELECT MAX(commit_id) FROM synclite_txn";
	protected static final String cleanupTxnTableSql = "DELETE FROM synclite_txn WHERE commit_id < ?";

	@Override
	public HashMap<String, Long> initReadCommitID(Path dbPath, long pageSize) throws SQLException {
		//initMetadataFile first.
		Path syncLiteDirPath = Path.of(dbPath.toString() + ".synclite");
		Path metadataDBPath = syncLiteDirPath.resolve(dbPath.getFileName().toString() + ".sqlite");
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataDBPath)) {
			try (Statement stmt = conn.createStatement()) {				
				stmt.execute(createTxnTableIfNotExistsSql);
				try (ResultSet rs = stmt.executeQuery(selectTxnTableSql)) {
					if (! rs.next()) {
						stmt.execute(insertTxnTable);
					}
				}
			}
		}    	

		long maxCommitID = 0;
		HashMap<String, Long> result = new HashMap<String, Long>(2);		
		try (Connection conn = getSrcConnection(dbPath)) {
			try (Statement stmt = conn.createStatement()) {
				if (nativeSupportsIfClause()) {
					stmt.execute(createTxnTableIfNotExistsSql);
				} else {
					try {
						stmt.execute(createTxnTableSql);
					} catch (SQLException e) {
						if (objectAlreadyExistsError(e)) {
							//Ignore
						} else {
							throw e;
						}
					}
				}
				try (ResultSet rs = stmt.executeQuery(selectMaxTxnTableSql)) {
					if (rs.next()) {
						maxCommitID = rs.getLong(1);
					}
				}

				String deleteSql = cleanupTxnTableSql.replace("?", String.valueOf(maxCommitID));
				stmt.execute(deleteSql);

				try (ResultSet rs = stmt.executeQuery(selectTxnTableSql)) {
					if (rs.next()) {
						result.put("commit_id", rs.getLong(1));
						result.put("operation_id", rs.getLong(2));
					} else {
						result.put("commit_id", 0L);
						result.put("operation_id", 0L);
						stmt.execute(insertTxnTable);
					}
				}
				conn.commit();
			}
		}		
		return result;
	}


	protected boolean objectAlreadyExistsError(SQLException e) {
		return false;
	}


	@Override
	public void backupDB(Path srcDB, Path dstDB, SyncLiteOptions options, boolean schemaOnly) throws SQLException {
		try {
			if (Files.exists(dstDB)) {
				Files.delete(dstDB);
			}
		} catch(IOException e) {
			throw new SQLException("Failed to cleanup existing destination file : " + dstDB + " : " + e.getMessage(), e);			
		}
		String sqliteUrl = "jdbc:sqlite:" + dstDB;
		long confBatchSize = options.getLogSegmentFlushBatchSize();

		try (Connection srcConn = getSrcConnection(srcDB);
				Connection dstConn = DriverManager.getConnection(sqliteUrl);
				Statement srcStmt = srcConn.createStatement();
				Statement dstStmt = dstConn.createStatement()) {

			// Disable auto-commit for SQLite
			dstConn.setAutoCommit(false);

			// Get the list of tables from DB
			for (String tableName : getSrcTables(srcConn)) {
				HashMap<Integer, Column> tableInfo = getSrcTableInfo(tableName, srcConn);	
				
				StringBuilder createTableSqlBuilder = new StringBuilder();
				StringBuilder colListBuilder = new StringBuilder();
				StringBuilder valListBuilder = new StringBuilder();
				
				createTableSqlBuilder.append("CREATE TABLE IF NOT EXISTS " + tableName + "(");
				boolean first = true;
				for (int i=0 ; i<tableInfo.size(); ++i) {
					Column c = tableInfo.get(i);
					String notNull = (c.isNullable == true) ? "NULL" : "NOT NULL";
					if (!first) {
						createTableSqlBuilder.append(",");
						colListBuilder.append(",");
						valListBuilder.append(",");
					}
					String defaultValue = (c.defaultValue != null) ? "DEFAULT(" + c.defaultValue + ")" : "";
					String primaryKey = (c.isPrimaryKey == true) ? "PRIMARY KEY" : "";
					
					createTableSqlBuilder.append(c.name).append(" ").append(c.type).append(" ")
					.append(notNull).append(" ").append(defaultValue).append(" ").append(primaryKey);

					colListBuilder.append(c.name);
					valListBuilder.append("?");
					first = false;
				}
				createTableSqlBuilder.append(")");

				// Create table in SQLite
				dstStmt.execute(createTableSqlBuilder.toString());

				if (!schemaOnly) {
					// Prepare insert statement for SQLite
					String insertSql = "INSERT INTO " + tableName + "(" + colListBuilder.toString() + ") VALUES (" + valListBuilder + ")";
					String selectSql = "SELECT " + colListBuilder + " FROM " + tableName;
					
					try (ResultSet tableData = srcStmt.executeQuery(selectSql)) {

						// Use PreparedStatement and batch insert for better performance
						try (PreparedStatement pstmt = dstConn.prepareStatement(insertSql)) {
							long batchSize = 0;
							while (tableData.next()) {
								// Clear previous parameters
								pstmt.clearParameters();

								// Set parameters based on column index (1-based)
								int columnCount = tableData.getMetaData().getColumnCount();
								for (int i = 1; i <= columnCount; i++) {
									pstmt.setObject(i, tableData.getObject(i));
								}

								// Add batch
								pstmt.addBatch();
								batchSize++;

								if ((batchSize % confBatchSize == 0) || !tableData.next()) {
									pstmt.executeBatch();
									batchSize = 0;
								}
							}
						}
					}
				}
			}		
			dstConn.commit();
			srcConn.rollback();
		} catch (SQLException e) {
			throw new SQLException("Failed to take a backup of database : " + srcDB +  " : " + e.getMessage(), e);
		}
	}

	protected abstract HashMap<Integer, Column> getSrcTableInfo(String tableName, Connection conn) throws SQLException;

	protected abstract List<String> getSrcTables(Connection srcConn) throws SQLException;

	protected abstract boolean nativeSupportsIfClause() throws SQLException;

	protected abstract Connection getSrcConnection(Path srcDB) throws SQLException;

	@Override
	public void processBackupDB(Path backupPath, SyncLiteOptions options) throws SQLException {
		List<String> includeTables = options.getIncludeTables();
		List<String> excludeTables = options.getExcludeTables();

		String tableInfoReaderSql = "SELECT tbl_name FROM sqlite_master WHERE type = 'table'";
		String url = "jdbc:sqlite:" + backupPath;

		List<String> dropTables = new ArrayList<String>();
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				//Just handle main database for now
				
				try (ResultSet rs = stmt.executeQuery(tableInfoReaderSql)) {
					String tableName = rs.getString(1);
					if ((includeTables !=null) && (!includeTables.contains(tableName))) {
						dropTables.add(tableName);
					}
					if ((excludeTables !=null) && (excludeTables.contains(tableName))) {
						dropTables.add(tableName);
					}
				}

				for (String tableName : dropTables) {
					try {
						if (nativeSupportsIfClause()) {						
							stmt.execute("DROP TABLE IF EXISTS " + tableName);
						} else {
							stmt.execute("DROP TABLE " + tableName);
						}
					} catch (SQLException e) {
						//Ignore
					}
				}
			}
		} catch(Exception e) {
			throw new SQLException("SyncLite : Failed to process initial data backup file for the supplied include/exclude list : " + e.getMessage(), e);
		}			
	}

}
