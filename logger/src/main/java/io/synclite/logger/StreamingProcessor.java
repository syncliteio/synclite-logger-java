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
