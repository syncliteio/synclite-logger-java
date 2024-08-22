package io.synclite.logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4Statement;

public class TelemetryStatement extends JDBC4Statement {
	private SQLLogger sqlLogger;
	protected TelemetryStatement(SQLiteConnection conn) throws SQLException {
		super(conn);
		this.sqlLogger = EventLogger.findInstance(getConn().getPath());
	}

	protected TelemetryConnection getConn() {
		return ((TelemetryConnection ) this.conn);
	}

	private final void logOper(String sql) throws SQLException {
		logOper(sql, null);
	}	

	protected void logOper(String sql, Object[] args) throws SQLException {
		long commitId = getConn().getCommitId();
		sqlLogger.log(commitId, sql, args);
	}

	public final void log(String msg) throws SQLException {
		logOper(msg);
		processCommit();
	}
	
	public final void log(String msg, Object[] args) throws SQLException {
		logOper(msg, args);
		processCommit();
	}
	
	private final Path logDataFile(Path dataFilePath) throws SQLException {
		return sqlLogger.logDataFile(dataFilePath);
	}

	public final boolean executeUnlogged(String sql) throws SQLException {
		boolean result = super.execute(sql);
		if (getConn().getUserAutoCommit() == true) {
			getConn().superCommit();
		}
		return result;
	}

	@Override
	public final boolean execute(String sql) throws SQLException {
		boolean result = false;
		List<String> sqls = SyncLiteUtils.splitSqls(sql);
		for (int i=0; i<sqls.size(); ++i) {
			result = executeSingleSQL(sqls.get(i));
		}
		return result;
	}

	private final boolean executeSingleSQL(String sql) throws SQLException {
		boolean result = false;
		String tokens[] = sql.trim().split("\\s+");
		if (tokens[0].equalsIgnoreCase("INSERT") && tokens[1].equalsIgnoreCase("INTO")) {
			result = executeInsert(sql);
		} else if (tokens[0].equalsIgnoreCase("DELETE") && tokens[1].equalsIgnoreCase("FROM")) {
			result = executeDelete(sql);
		} else if (tokens[0].equalsIgnoreCase("COPY")) {
			result = executeCopySql(sql);
		} else if ((tokens[0].equalsIgnoreCase("CREATE") || tokens[0].equalsIgnoreCase("DROP")) &&
				(tokens[1].equalsIgnoreCase("TABLE"))
		) {
				result = executeDDL(sql);
		} else if (tokens[0].equalsIgnoreCase("ALTER") && tokens[1].equalsIgnoreCase("TABLE")) {
			if (tokens.length < 6) {
				throw new SQLException("Invalid SQL : " + sql);
			} else { 
				if (tokens[3].equalsIgnoreCase("ALTER") && tokens[4].equalsIgnoreCase("COLUMN")) {
					result = executeAlterColumnDDL(sql, tokens);
				} else {
					result = executeDDL(sql);
				}
			}	
		} else if (tokens[0].equalsIgnoreCase("REFRESH") && tokens[1].equalsIgnoreCase("TABLE")) {	
			result = executeRefreshTable(sql, tokens);
		} else if (tokens[0].equalsIgnoreCase("PUBLISH") && tokens[1].equalsIgnoreCase("COLUMN") && tokens[2].equalsIgnoreCase("LIST")) {	
			result = executePublishColumnList(sql, tokens);
		} else if (tokens[0].equalsIgnoreCase("UPDATE")) {
			throw new SQLException("Unsupported SQL : SyncLite telemetry device does not support SQL : " + sql + ". Supported SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, COPY, SELECT");
		} else {
			try {
				SyncLiteUtils.checkAndExecuteInternalTelemetrySql(sql);
			} catch (SQLException e) {
				throw new SQLException("Unsupported SQL : SyncLite telemetry device does not support SQL : " + sql + ". Supported SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, COPY, SELECT");
			}
		}    	
		return result;
	}

	private boolean executeAlterColumnDDL(String sql, String[] tokens) throws SQLException {
		//
		//SQLite does not support alter column
		//We are supporting this only for telemetry device as below 
		//1. Drop column (unlogged)
		//2. Create Column (unlogged)
		//3. log ALTER COLUMN DDL
		//4. Process Transaction
		//
		String tableName = tokens[2];
		String columnName = tokens[5];
		StringBuilder colDef = new StringBuilder();
		for (int i = 6; i < tokens.length; ++i) {
			colDef.append(" ");
			colDef.append(tokens[i]);
		}
		String dropColumnSql = "ALTER TABLE " + tableName + " DROP COLUMN " + columnName;
		String addColumnSql = "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + colDef.toString();
		boolean result = super.execute(dropColumnSql);
		result = super.execute(addColumnSql);
		logOper(sql);
		processCommit();
		return result;
	}

	private boolean executeRefreshTable(String sql, String[] tokens) throws SQLException {
		try {
			String[] subTokens = tokens[2].split("\\(");
			if (subTokens.length == 0) {
				throw new SQLException("Invalid Refresh table SQL : ", sql);
			}
			String dropTableSql = "DROP TABLE IF EXISTS " + tokens[2].split("\\(")[0].strip();
			boolean result = super.execute(dropTableSql);		
			tokens[0] = "CREATE";
			String createTableSql = String.join(" ", tokens);
			result = super.execute(createTableSql);
			logOper(sql);
			processCommit();
			return result;
		} catch (Exception e) {
			throw new SQLException("Failed to execute refresh table sql : " + sql, e);
		}
	}

	private boolean executePublishColumnList(String sql, String[] tokens) throws SQLException {
		try {
			String tableName = tokens[3];
			//Get column list
			StringBuilder colList = new StringBuilder();
			boolean first = true;
			try (ResultSet rs = super.executeQuery("pragma table_info(" + tableName + ")")) {
				while (rs.next()) {
					if (!first) {
						colList.append(",");
					}
	                String columnName = rs.getString("name");
	                colList.append(columnName);
	                first = false;
				} 
			}
			sql = sql + " " + colList.toString();
			log(sql);
			processCommit();
			return true;
		} catch (Exception e) {
			throw new SQLException("Failed to execute refresh column list sql: " + sql, e);
		}
	}

	private final boolean executeDDL(String sql) throws SQLException {
		boolean result = super.execute(sql);
		logOper(sql);
		processCommit();
		return result;
	}

	private final boolean executeInsert(String sql) throws SQLException {
		logOper(sql);
		processCommit();
		return true;
	}

	private final boolean executeDelete(String sql) throws SQLException {
		logOper(sql);
		processCommit();
		return true;
	}

	private final boolean executeCopySql(String sql) throws SQLException {
		//Parse copy command
		//COPY <table> FROM <FilePath> <options>

		sql = sql.trim();
		String []tokens = sql.split("\\s+");
		String tableName;
		String csvPath;
		String format = "CSV";
		char delimiter = ',';
		String nullString = "null";
		boolean header = true;
		char quote = '"';
		char escape = '"';

		if (!tokens[0].equalsIgnoreCase("COPY")) {
			throw new SQLException("SyncLite : Parse Error, incorrect syntax near " + tokens[0] + ", expected keyword COPY");
		}

		tableName= tokens[1];

		if (!tokens[2].equalsIgnoreCase("FROM")) {
			throw new SQLException("SyncLite : Parse Error, incorrect syntax near " + tokens[2] + ", expected keyword FROM");
		}

		if (tokens[3].indexOf("'") < tokens[3].lastIndexOf("'")) {
			csvPath = tokens[3].substring(tokens[3].indexOf("'")+1 , tokens[3].lastIndexOf("'"));
		} else {
			throw new SQLException("SyncLite : COPY statement, specify a valid CSV file path in single quotes.");
		}

		if (!Files.exists(Path.of(csvPath))) {
			throw new SQLException("SyncLite : COPY statement, specified file : " + csvPath + " does not exist");
		}

		if (!Path.of(csvPath).toFile().canRead()) {
			throw new SQLException("SyncLite : COPY statement, specified file : " + csvPath + " does not have READ permission");
		}

		for (int i=4; i < tokens.length; ++i) {
			String optName = tokens[i].toUpperCase();
			switch(optName) {
			case "FORMAT":
				format = tokens[i+1].toUpperCase();
				if (!format.equals("CSV")) {
					throw new SQLException("SyncLite : COPY statement, unsupported file format : " + format);
				}
				++i;
				break;
			case "DELIMITER":
				if ((tokens[i+1].length() == 3) && (tokens[i+1].charAt(0) == '\'') && (tokens[i+1].charAt(2) == '\'')) {
					delimiter = tokens[i+1].charAt(1);
				} else {
					throw new SQLException("SyncLite : COPY statement, invalid delimiter specification : " + tokens[i+1]);
				}
				++i;
				break;
			case "NULLSTRING":
				nullString = tokens[i+1];
				++i;
				break;
			case "HEADER":
				try {
					header = Boolean.valueOf(tokens[i+1]);
				} catch (Exception e) {
					throw new SQLException("SyncLite : COPY statement, invalid header specification : " + tokens[i+1] + ", expected true/false");
				}
				++i;
				break;
			case "QUOTE":
				if ((tokens[i+1].length() == 3) && (tokens[i+1].charAt(0) == '\'') && (tokens[i+1].charAt(2) == '\'')) {
					quote = tokens[i+1].charAt(1);
				} else {
					throw new SQLException("SyncLite : COPY statement, invalid quote specification : " + tokens[i+1]);
				}					
				++i;
				break;
			case "ESCAPE":
				if ((tokens[i+1].length() == 3) && (tokens[i+1].charAt(0) == '\'') && (tokens[i+1].charAt(2) == '\'')) {
					escape = tokens[i+1].charAt(1);
				} else {
					throw new SQLException("SyncLite : COPY statement, invalid escape specification : " + tokens[i+1]);
				}
				++i;
				break;
			default:
				throw new SQLException("SyncLite : COPY statement, unsupported option : " + optName);
			}
		}

		Path loggedDataFile = logDataFile(Path.of(csvPath));
		Object[] args = new Object[8];
		args[0] = tableName;
		args[1] = loggedDataFile.getFileName();
		args[2] = format;
		args[3] = delimiter;
		args[4] = nullString;
		args[5] = header;
		args[6] = quote;
		args[7] = escape;

		logOper(sql, args);
		processCommit();
		return true;
	}

	@Override
	public final ResultSet executeQuery(String sql) throws SQLException {
		ResultSet rs = null;		
		List<String> sqls = SyncLiteUtils.splitSqls(sql);
		for (int i=0; i<sqls.size(); ++i) {
			try {
				executeSingleSQL(sqls.get(i));
			} catch(SQLException e) {
				if (e.getMessage().startsWith("Unsupported SQL")) {
					String tokens[] = sql.trim().split("\\s+");
					if (tokens[0].equalsIgnoreCase("SELECT")) {
						rs = super.executeQuery(sql);
					} else {
						throw e;
					}

				}
			}
		}
		return rs;
	}

	@Override
	public final int executeUpdate(String sql) throws SQLException {
		int rt = execute(sql) ? 1 : 0;
		return rt;
	}

	private final void processCommit() throws SQLException {
		if (getConn().getUserAutoCommit() == true) {
			getConn().commit();
		}
	}
}
