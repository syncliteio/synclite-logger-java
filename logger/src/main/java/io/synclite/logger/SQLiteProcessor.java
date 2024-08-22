package io.synclite.logger;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SQLiteProcessor extends DBProcessor {
	
	public SQLiteProcessor() {		
	}

	@Override
	public HashMap<String, Long> initReadCommitID(Path dbPath, long pageSize) throws SQLException {
		HashMap<String, Long> result = new HashMap<String, Long>(2);		
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
			try (Statement stmt = conn.createStatement()) {
				long defaultDevicePageSize = pageSize;
				if (defaultDevicePageSize > 0) {
					stmt.execute("pragma page_size=" + defaultDevicePageSize);
				}
				stmt.execute(createTxnTableIfNotExistsSql);
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
			}
		}		
		return result;
	}

	@Override
	public void backupDB(Path srcDB, Path dstDB, SyncLiteOptions options, boolean schemaOnly) throws SQLException {
		String url = "jdbc:sqlite:" + srcDB;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				//TODO check if BACKUP TO overrites any existing file at backupPath 
				stmt.executeUpdate("BACKUP TO '" + dstDB + "'");
			}
		}
	}
	
	@Override
	public void processBackupDB(Path backupPath, SyncLiteOptions options) throws SQLException {
		List<String> includeTables = options.getIncludeTables();
		List<String> excludeTables = options.getExcludeTables();

		String tableInfoReaderSql = "SELECT tbl_name FROM sqlite_master WHERE type = 'table'";
		//String tableInfoReaderSql = "SELECT m.name, p.\"table\" FROM sqlite_master m LEFT JOIN pragma_foreign_key_list(m.name) p ON m.name != p.\"table\" WHERE m.type = 'table'";
		String url = "jdbc:sqlite:" + backupPath;
		List<String> dropTables = new ArrayList<String>();
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				//Just handle main database for now
				try (ResultSet rs = stmt.executeQuery(tableInfoReaderSql)) {
					while(rs.next()) {
						String tableName = rs.getString(1);
						if ((includeTables !=null) && (!includeTables.contains(tableName))) {
							dropTables.add(tableName);
						}
						if ((excludeTables !=null) && (excludeTables.contains(tableName))) {
							dropTables.add(tableName);
						}
					}
				}
				stmt.execute("pragma foreign_keys = OFF;");
				for (String tableName : dropTables) {
					stmt.execute("DROP TABLE IF EXISTS " + tableName);
				}
				if (options.getVacuumDataBackup()) {
					stmt.execute("vacuum");
				}
			}

		} catch(Exception e) {
			throw new SQLException("SyncLite : Failed to process initial data backup file for the supplied include/exclude list : " + e.getMessage(), e);
		}
	}
	
}
