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

public class DuckDBProcessor extends MultiWriterDBProcessor {

	public DuckDBProcessor() {		
	}

	@Override
	protected HashMap<Integer, Column> getSrcTableInfo(String tableName, Connection conn) throws SQLException {
		HashMap<Integer, Column> cols = new HashMap<Integer, Column>();
		try (Statement stmt = conn.createStatement()) {
			int colIndex = 0;
			try (ResultSet tableSchema = stmt.executeQuery("PRAGMA table_info(" + tableName + ")")) {
				while (tableSchema.next()) {
					String columnName = tableSchema.getString("name");
					String columnType = tableSchema.getString("type");
					String type = columnType;
					if (columnType.contains("[")) {
						//If the column type contains array subscript then get the left side of [ and set data type with empty array subscript
						//E.g. float[5] ==> float[]
						String[] tokens = columnType.split("[");
						type = tokens[0].strip().toUpperCase() + "[]";
					}
					boolean isNullable = tableSchema.getBoolean("notnull") == true ? false : true;
					String defaultValue = tableSchema.getString("dflt_value") != null ? tableSchema.getString("dflt_value") : null;
					boolean primaryKey = tableSchema.getBoolean("pk") == true ? true  : false;
					
					Column c = new Column(columnName, type, defaultValue, isNullable, primaryKey);
					cols.put(colIndex, c);
					++colIndex;
				}
			} catch (SQLException e) {
				if (!e.getMessage().contains("does not exist")) {
					throw e;
				}
			}
		}
		return cols;
	}

	@Override
	protected List<String> getSrcTables(Connection srcConn) throws SQLException {
		List<String> tableList = new ArrayList<String>();
		try(Statement stmt = srcConn.createStatement()) {
			try (ResultSet tables = stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='main'")) {
				while(tables.next()) {
					tableList.add(tables.getString("table_name"));
				}
			}
		}
		return tableList;
	}

	@Override
	protected boolean nativeSupportsIfClause() throws SQLException {
		return true;
	}

	@Override
	protected Connection getSrcConnection(Path srcDB) throws SQLException {
		String duckDBUrl = "jdbc:duckdb:" + srcDB;
		Connection conn = DriverManager.getConnection(duckDBUrl);
		conn.setAutoCommit(false);
		return conn;
	}
}
