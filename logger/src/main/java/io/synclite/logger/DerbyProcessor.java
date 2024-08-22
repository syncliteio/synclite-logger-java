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

import java.nio.file.Files;
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

public class DerbyProcessor extends MultiWriterDBProcessor {

	public DerbyProcessor() {		
	}

	@Override
	protected HashMap<Integer, Column> getSrcTableInfo(String tableName, Connection conn) throws SQLException {
		HashMap<Integer, Column> cols = new HashMap<Integer, Column>();
		String tableInfoReaderSql = "SELECT c.COLUMNNAME, c.COLUMNDATATYPE, c.COLUMNDEFAULT FROM SYS.SYSTABLES t JOIN SYS.SYSCOLUMNS c ON t.TABLEID = c.REFERENCEID WHERE t.TABLENAME ='" +  tableName + "' ORDER BY c.COLUMNNUMBER";
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
					String columnName = tableSchema.getString("COLUMNNAME");
					String columnType = tableSchema.getString("COLUMNDATATYPE");

					//TODO find nullability for derby columns
					boolean isNullable = false;
					String defaultValue = tableSchema.getString("COLUMNDEFAULT") != null ? tableSchema.getString("COLUMNDEFAULT") : null;
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
			try (ResultSet tables = stmt.executeQuery("SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE = 'T'")) {
				while(tables.next()) {
					tableList.add(tables.getString("TABLENAME"));
				}
			}
		}
		return tableList;
	}

	@Override
	protected boolean nativeSupportsIfClause() throws SQLException {
		return false;
	}

	@Override
	protected Connection getSrcConnection(Path srcDB) throws SQLException {
		String url = "jdbc:derby:" + srcDB + ";readonly=true";
		if (! Files.exists(srcDB)) {
			url = "jdbc:derby:" + srcDB + ";create=true;readonly=true";
		}
		Connection conn = DriverManager.getConnection(url);
		conn.setAutoCommit(false);
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		return conn;
	}

	@Override
	protected boolean objectAlreadyExistsError(SQLException e) {
		return (e.getMessage().contains("already exists"));
	}

}
