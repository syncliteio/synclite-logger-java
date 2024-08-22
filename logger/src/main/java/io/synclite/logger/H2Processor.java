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

public class H2Processor extends MultiWriterDBProcessor {

	public H2Processor() {		
	}

	@Override
	protected HashMap<Integer, Column> getSrcTableInfo(String tableName, Connection conn) throws SQLException {
		HashMap<Integer, Column> cols = new HashMap<Integer, Column>();
		String tableInfoReaderSql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS where table_name = '" + tableName + "' ORDER BY ORDINAL_POSITION";
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
					String type = tableSchema.getString("DATA_TYPE");
					int precision = tableSchema.getInt("NUMERIC_PRECISION");
					int scale = tableSchema.getInt("NUMERIC_SCALE");
					int maxLen = tableSchema.getInt("CHARACTER_MAXIMUM_LENGTH");

					String columnType = tableSchema.getString("DATA_TYPE");
					switch(type) {
					case "DECIMAL":
					case "NUMBER":
					case "NUMERIC":
						columnType += "(" + precision + "," + scale + ")";
					case "CHAR":
					case "VARCHAR":
					case "VARCHAR2":
						columnType += "(" + maxLen + ")";
					}

					boolean isNullable = tableSchema.getString("IS_NULLABLE").equals("NO")? false : true;
					String defaultValue = tableSchema.getString("COLUMN_DEFAULT") != null ? tableSchema.getString("COLUMN_DEFAULT") : null;
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
			String tableInfoReaderSql = "SELECT table_name FROM information_schema.tables WHERE table_schema='PUBLIC';";
			try (ResultSet tables = stmt.executeQuery(tableInfoReaderSql)) {
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
		String url = "jdbc:h2:" + srcDB;
		Connection conn = DriverManager.getConnection(url);
		conn.setAutoCommit(false);
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		return conn;
	}

}
