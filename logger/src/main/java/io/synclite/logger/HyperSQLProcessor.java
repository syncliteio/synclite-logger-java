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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HyperSQLProcessor extends MultiWriterDBProcessor {

	public HyperSQLProcessor() {		
	}

	@Override
	protected HashMap<Integer, Column> getSrcTableInfo(String tableName, Connection conn) throws SQLException {
		HashMap<Integer, Column> cols = new HashMap<Integer, Column>();
		String tableInfoReaderSql = "SELECT COLUMN_NAME, TYPE_NAME, COLUMN_DEF, IS_NULLABLE, CASE  WHEN COLUMN_NAME IN (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.SYSTEM_PRIMARYKEYS WHERE TABLE_NAME = '" + tableName + "') THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME = '" + tableName + "' ORDER BY ORDINAL_POSITION";
		try (Statement stmt = conn.createStatement()) {
			DatabaseMetaData metadata = conn.getMetaData();
			Set<String> pkCols = new HashSet<String>();
			ResultSet pkSet = metadata.getPrimaryKeys(null, "PUBLIC", tableName);
			while(pkSet.next()) {
				pkCols.add(pkSet.getString(4));
			}

			int colIndex = 0;
			try (ResultSet tableSchema = stmt.executeQuery(tableInfoReaderSql)) {
				while (tableSchema.next()) {
					String columnName = tableSchema.getString("COLUMN_NAME");
					String columnType = tableSchema.getString("TYPE_NAME");
					boolean isNullable = tableSchema.getString("IS_NULLABLE").equals("NO")? false : true;
					String defaultValue = tableSchema.getString("COLUMN_DEF") != null ? tableSchema.getString("COLUMN_DEFAULT") : null;
					boolean isPrimaryKey = pkCols.contains(columnName) ? true: false;
					Column c = new Column(columnName, columnType, defaultValue, isNullable, isPrimaryKey);
					cols.put(colIndex, c);
					++colIndex;
				}
			}
		}
		return cols;
	}

		@Override
		protected List<String> getSrcTables(Connection srcConn) throws SQLException {
			List<String> tableList = new ArrayList<String>();
			try(Statement stmt = srcConn.createStatement()) {
				String tableInfoReaderSql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_TABLES WHERE TABLE_TYPE = 'TABLE' AND TABLE_SCHEM = 'PUBLIC'";
				try (ResultSet tables = stmt.executeQuery(tableInfoReaderSql)) {
					while(tables.next()) {
						tableList.add(tables.getString("TABLE_NAME"));
					}
				}
			}
			return tableList;
		}

		@Override
		protected boolean nativeSupportsIfClause() throws SQLException {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		protected Connection getSrcConnection(Path srcDB) throws SQLException {
			String url = "jdbc:hsqldb:file:" + srcDB;
			Connection conn = DriverManager.getConnection(url);
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			return conn;
		}
	}
