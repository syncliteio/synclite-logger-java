package io.synclite.logger;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class StreamingProcessor extends SQLiteProcessor {

	protected static final String selectMaxTxnTableSql = "SELECT MAX(commit_id) FROM synclite_txn";
	protected static final String cleanupTxnTableSql = "DELETE FROM synclite_txn WHERE commit_id < ?";

	@Override
	public HashMap<String, Long> initReadCommitID(Path dbPath, long pageSize) throws SQLException {
		long maxCommitID = 0;
		HashMap<String, Long> result = new HashMap<String, Long>(2);		
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
			try (Statement stmt = conn.createStatement()) {
				long defaultDevicePageSize = pageSize;
				if (defaultDevicePageSize > 0) {
					stmt.execute("pragma page_size=" + defaultDevicePageSize);
				}
				stmt.execute(createTxnTableIfNotExistsSql);
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
			}
		}		
		return result;
	}

}
