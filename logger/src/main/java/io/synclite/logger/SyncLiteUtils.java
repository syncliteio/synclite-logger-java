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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.sqlite.SQLiteConnection;

import io.synclite.logger.MultiWriterDBProcessor.Column;

public class SyncLiteUtils {

	// Define the regular expression pattern for the allowed INSERT syntaxes
	private static final String INSERT_DBLOGGER_APPENDER_PATTERN_STR = "INSERT\\s+INTO\\s+(\\w+\\.)?\\w+\\s*(?:\\(([^)]+)\\))?\\s*VALUES\\s*\\(([^)]+)\\)";

	// UPDATE: requires SET; WHERE is optional; blocks subqueries and function/UDF calls
	private static final String UPDATE_DBLOGGER_APPENDER_PATTERN_STR = "UPDATE\\s+(\\w+\\.)?\\w+\\s+SET\\s+(?!.*\\bSELECT\\b)(?!.*\\w\\s*\\().+";

	// DELETE: WHERE is optional; blocks subqueries and function/UDF calls
	private static final String DELETE_DBLOGGER_APPENDER_PATTERN_STR = "DELETE\\s+FROM\\s+(\\w+\\.)?\\w+(?:\\s+WHERE\\s+(?!.*\\bSELECT\\b)(?!.*\\w\\s*\\().+)?";

	// Create a Pattern object
	private static final Pattern INSERT_DBLOGGER_APPENDER_PATTERN = Pattern.compile(INSERT_DBLOGGER_APPENDER_PATTERN_STR, Pattern.CASE_INSENSITIVE);

	private static final Pattern UPDATE_DBLOGGER_APPENDER_PATTERN = Pattern.compile(UPDATE_DBLOGGER_APPENDER_PATTERN_STR, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	private static final Pattern DELETE_DBLOGGER_APPENDER_PATTERN = Pattern.compile(DELETE_DBLOGGER_APPENDER_PATTERN_STR, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	// Patterns for getTableNameFromDDL — hoisted to avoid recompiling on every call
	private static final Pattern DDL_CREATE_TABLE_PATTERN = Pattern.compile("CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:[\\w.]+\\.)?([\\w]+)", Pattern.CASE_INSENSITIVE);
	private static final Pattern DDL_DROP_TABLE_PATTERN   = Pattern.compile("DROP\\s+TABLE\\s+(?:IF\\s+EXISTS\\s+)?(?:[\\w.]+\\.)?([\\w]+)", Pattern.CASE_INSENSITIVE);
	private static final Pattern DDL_ALTER_TABLE_PATTERN  = Pattern.compile("ALTER\\s+TABLE\\s+(?:[\\w.]+\\.)?([\\w]+)", Pattern.CASE_INSENSITIVE);

	// Pattern to extract table name from INSERT INTO [db.]table ...
	private static final Pattern INSERT_TABLE_NAME_PATTERN = Pattern.compile("INSERT\\s+INTO\\s+(?:\\w+\\.)?([\\w]+)", Pattern.CASE_INSENSITIVE);

	// Patterns for getDDLStatement ALTER sub-clauses — hoisted to avoid recompiling on every call
	private static final Pattern DDL_ADD_COLUMN_PATTERN    = Pattern.compile("ADD\\s+(?:COLUMN)?", Pattern.CASE_INSENSITIVE);
	private static final Pattern DDL_DROP_COLUMN_PATTERN   = Pattern.compile("DROP\\s+(?:COLUMN)?", Pattern.CASE_INSENSITIVE);
	private static final Pattern DDL_ALTER_COLUMN_PATTERN  = Pattern.compile("ALTER\\s+(?:COLUMN)?", Pattern.CASE_INSENSITIVE);
	private static final Pattern DDL_RENAME_TO_PATTERN     = Pattern.compile("RENAME\\s+TO", Pattern.CASE_INSENSITIVE);
	private static final Pattern DDL_RENAME_COLUMN_PATTERN = Pattern.compile("RENAME\\s+(?:COLUMN)?", Pattern.CASE_INSENSITIVE);
	private static final String PROTECTED_TXN_TABLE_NAME = "SYNCLITE_TXN";

	static final List<String> splitSqls(String sql) {
		List<String> sqls = new ArrayList<String>();
		StringBuilder currentSqlBuilder = new StringBuilder();
		char[] inputChars = sql.toCharArray();
		boolean insideSingleQuotedString = false;
		boolean insideDoubleQuotedString = false;
		boolean insideLineComment = false;
		boolean insideBlockComment = false;
		for (int i=0; i < inputChars.length; ++i) {
			// -- line comment: skip until end of line
			if (insideLineComment) {
				if (inputChars[i] == '\n') {
					insideLineComment = false;
				}
				// characters inside a line comment are not appended
				continue;
			}
			// /* */ block comment: skip until */
			if (insideBlockComment) {
				if (inputChars[i] == '*' && (i+1) < inputChars.length && inputChars[i+1] == '/') {
					insideBlockComment = false;
					++i; // skip the '/'
				}
				// characters inside a block comment are not appended
				continue;
			}
			if (insideSingleQuotedString) {
				if (inputChars[i] == '\'') {
					// '' is an escaped single quote; otherwise it closes the string
					currentSqlBuilder.append(inputChars[i]);
					if ((i+1) < inputChars.length) {
						if (inputChars[i+1] == '\'') {
							currentSqlBuilder.append(inputChars[i+1]);
							++i;
						} else {
							insideSingleQuotedString = false;
						}
					} else {
						// closing quote is the last character in input
						insideSingleQuotedString = false;
					}
				} else {
					currentSqlBuilder.append(inputChars[i]);
				}
			} else if (insideDoubleQuotedString) {
				if (inputChars[i] == '\"') {
					// "" is an escaped double quote; otherwise it closes the identifier
					currentSqlBuilder.append(inputChars[i]);
					if ((i+1) < inputChars.length) {
						if (inputChars[i+1] == '\"') {
							currentSqlBuilder.append(inputChars[i+1]);
							++i;
						} else {
							insideDoubleQuotedString = false;
						}
					} else {
						// closing quote is the last character in input
						insideDoubleQuotedString = false;
					}
				} else {
					currentSqlBuilder.append(inputChars[i]);
				}
			} else {
				// check for comment starts before any other dispatch
				if (inputChars[i] == '-' && (i+1) < inputChars.length && inputChars[i+1] == '-') {
					insideLineComment = true;
					++i; // skip the second '-'
				} else if (inputChars[i] == '/' && (i+1) < inputChars.length && inputChars[i+1] == '*') {
					insideBlockComment = true;
					++i; // skip the '*'
				} else if (inputChars[i] == ';') {
					String nextSql = currentSqlBuilder.toString();
					if (!nextSql.isBlank()) {
						sqls.add(nextSql.trim());
					}
					currentSqlBuilder = new StringBuilder();
				} else {
					currentSqlBuilder.append(inputChars[i]);
					if (inputChars[i] == '\'') {
						insideSingleQuotedString = true;
					} else if (inputChars[i] == '\"') {
						insideDoubleQuotedString = true;
					}
				}
			}
		}

		String nextSql = currentSqlBuilder.toString();
		if (!nextSql.isBlank()) {
			sqls.add(nextSql.trim());
		}
		return sqls;
	}

	static final long nextPowerOf2(long n)
	{
		if (n <= 0) return 1;
		n--;
		n |= n >> 1;
		n |= n >> 2;
		n |= n >> 4;
		n |= n >> 8;
		n |= n >> 16;
		n |= n >> 32;
		n++;
		return n;
	}

	final static String prepareArgList(long argCnt) {
		StringBuilder argListBuilder = new StringBuilder();
		for (long i = 1; i <= argCnt; ++i) {
			if (i > 1) {
				argListBuilder.append(", ");
			}
			argListBuilder.append("arg" + i);
		}
		return argListBuilder.toString();
	}

	final static String preparePStmtFillerList(long argCnt) {
		StringBuilder argListBuilder = new StringBuilder();
		for (long i = 1; i <= argCnt; ++i) {
			if (i > 1) {
				argListBuilder.append(", ");
			}
			argListBuilder.append("?");
		}
		return argListBuilder.toString();
	}


	final static boolean checkInternalSql(String sql) throws SQLException {
		//Check if this is an internal SQL supported by SyncLite
		//
		//CLOSE ALL DATABASES
		//CLOSE ALL DEVICES
		//CLOSE DATABASE <db/path>
		//CLOSE DEVICE <device/path>
		//
		String[] tokens = sql.split("\\s+");
		boolean validSql = false;
		if (tokens.length != 3) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		if (tokens[0].equalsIgnoreCase("CLOSE")) {
			if (tokens[1].equalsIgnoreCase("ALL")) {
				if (tokens[2].equalsIgnoreCase("DATABASES")) {
					validSql = true;
				} else if (tokens[2].equalsIgnoreCase("DEVICES")) {
					validSql = true;
				}
			} else if (tokens[1].equalsIgnoreCase("DATABASE")) {
				validSql = true;
			} else if (tokens[1].equalsIgnoreCase("DEVICE")) {
				validSql = true;
			}
		}		
		if (validSql == false) {
			throw new SQLException("SyncLite : Unsupported SQL : " + sql);
		}
		return false;
	}

	final static boolean checkAndExecuteInternalTransactionalSql(String sql) throws SQLException {
		//Check if this is an internal SQL supported by SyncLite
		//
		//CLOSE ALL DATABASES
		//CLOSE ALL DEVICES
		//CLOSE DATABASE <db/path>
		//CLOSE DEVICE <device/path>
		//
		String[] tokens = sql.split("\\s+");
		boolean validSql = false;
		if (tokens.length != 3) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		if (tokens[0].equalsIgnoreCase("CLOSE")) {
			if (tokens[1].equalsIgnoreCase("ALL")) {
				if (tokens[2].equalsIgnoreCase("DATABASES")) {
					validSql = true;
					SQLite.closeAllDatabases();
				} else if (tokens[2].equalsIgnoreCase("DEVICES")) {
					validSql = true;
					SQLite.closeAllDevices();
				}
			} else if (tokens[1].equalsIgnoreCase("DATABASE")) {
				validSql = true;
				SQLite.closeDatabase(Path.of(tokens[2]));
			} else if (tokens[1].equalsIgnoreCase("DEVICE")) {
				validSql = true;
				SQLite.closeDevice(Path.of(tokens[2]));
			}
		}		
		if (validSql == false) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		return false;
	}


	final static boolean checkAndExecuteInternalDBLoggerSql(String sql) throws SQLException {
		//Check if this is an internal SQL supported by SyncLite
		//
		//CLOSE ALL DATABASES
		//CLOSE ALL DEVICES
		//CLOSE DATABASE <db/path>
		//CLOSE DEVICE <device/path>
		//
		String[] tokens = sql.split("\\s+");
		boolean validSql = false;
		if (tokens.length != 3) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		if (tokens[0].equalsIgnoreCase("CLOSE")) {
			if (tokens[1].equalsIgnoreCase("ALL")) {
				if (tokens[2].equalsIgnoreCase("DATABASES")) {
					validSql = true;
					DBLogger.closeAllDatabases();
				} else if (tokens[2].equalsIgnoreCase("DEVICES")) {
					validSql = true;
					DBLogger.closeAllDevices();
				}
			} else if (tokens[1].equalsIgnoreCase("DATABASE")) {
				validSql = true;
				DBLogger.closeDatabase(Path.of(tokens[2]));
			} else if (tokens[1].equalsIgnoreCase("DEVICE")) {
				validSql = true;
				DBLogger.closeDevice(Path.of(tokens[2]));
			}
		}		
		if (validSql == false) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		return false;
	}


	final static boolean checkAndExecuteInternalStoreSql(String sql) throws SQLException {
		//Check if this is an internal SQL supported by SyncLite
		String[] tokens = sql.split("\\s+");
		boolean validSql = false;
		if (tokens.length != 3) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		if (tokens[0].equalsIgnoreCase("CLOSE")) {
			if (tokens[1].equalsIgnoreCase("ALL")) {
				if (tokens[2].equalsIgnoreCase("DATABASES")) {
					validSql = true;
					SQLiteStore.closeAllDatabases();
				} else if (tokens[2].equalsIgnoreCase("DEVICES")) {
					validSql = true;
					SQLiteStore.closeAllDevices();
				}
			} else if (tokens[1].equalsIgnoreCase("DATABASE")) {
				validSql = true;
				SQLiteStore.closeDatabase(Path.of(tokens[2]));
			} else if (tokens[1].equalsIgnoreCase("DEVICE")) {
				validSql = true;
				SQLiteStore.closeDevice(Path.of(tokens[2]));
			}
		}
		if (validSql == false) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		return false;
	}

	final static boolean checkAndExecuteInternalAppenderSql(String sql) throws SQLException {
		//Check if this is an internal SQL supported by SyncLite
		//
		//CLOSE ALL DATABASES
		//CLOSE ALL DEVICES
		//CLOSE DATABASE <db/path>
		//CLOSE DEVICE <device/path>
		//
		String[] tokens = sql.split("\\s+");
		boolean validSql = false;
		if (tokens.length != 3) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		if (tokens[0].equalsIgnoreCase("CLOSE")) {
			if (tokens[1].equalsIgnoreCase("ALL")) {
				if (tokens[2].equalsIgnoreCase("DATABASES")) {
					validSql = true;
					SQLiteAppender.closeAllDatabases();
				} else if (tokens[2].equalsIgnoreCase("DEVICES")) {
					validSql = true;
					SQLiteAppender.closeAllDevices();
				}
			} else if (tokens[1].equalsIgnoreCase("DATABASE")) {
				validSql = true;
				SQLiteAppender.closeDatabase(Path.of(tokens[2]));
			} else if (tokens[1].equalsIgnoreCase("DEVICE")) {
				validSql = true;
				SQLiteAppender.closeDevice(Path.of(tokens[2]));
			}
		}		
		if (validSql == false) {
			throw new SQLException("Unsupported SQL : " + sql);
		}
		return false;
	}

	public static String parsePositionalArgs(String sql, HashMap<Integer, Object> specialPositionalArgsMap) {
		//INSERT INTO tab1 VALUES(NULL/*AUTOINCREMENT*/, ?, ?, ?, ?)
		//INSERT INTO tab1 VALUES(?, NULL/*AUTOINCREMENT*/, ?, ?, ?)
		//INSERT INTO tab1 VALUES(?, ?, ?, ?, NULL/*AUTOINCREMENT*/)
		//
		Matcher matcher = INSERT_DBLOGGER_APPENDER_PATTERN.matcher(sql);

		if (!matcher.matches()) {
			return null;
		}

		int lastGroupIndex = matcher.groupCount();
		String argList = matcher.group(lastGroupIndex);
		int startIndex = matcher.start(lastGroupIndex);

		String[] args = argList.split(",");

		boolean hasSpecialPositionalArg = false;
		StringBuilder rewrittenSql = new StringBuilder();
		rewrittenSql.append(sql.substring(0, startIndex));
		int userSuppliedArgIndex = 0;
		for (int i = 0; i < args.length; ++i) {
			String arg = args[i].replaceAll("\\s", "").toUpperCase();
			if (!arg.equals("?")) {
				if (arg.equals("NULL/*AUTOINCREMENT*/")) {
					specialPositionalArgsMap.put(i, "AUTOINCREMENT");
				} else {
					specialPositionalArgsMap.put(i, arg);
				}
				hasSpecialPositionalArg = true;
			} else {				
				specialPositionalArgsMap.put(i, userSuppliedArgIndex);
				++userSuppliedArgIndex;
			}
			if (i == 0) {
				rewrittenSql.append("?");
			} else {
				rewrittenSql.append(",?");
			}
		}

		if (hasSpecialPositionalArg) {
			rewrittenSql.append(")");
			return rewrittenSql.toString();
		} else {
			//Empty the positionalArgMap
			specialPositionalArgsMap.clear();
		}
		return null;
	}

	//TODO
	//This method is far from complete
	//IF we go this route we need to handle all the SQLITE functions which can be used in 
	//INSERT statements.
	//
	static final Object generateValueForFunction(String func, long nextAutoIncrNum) {
		switch(func) {
		case "AUTOINCREMENT" :
			return nextAutoIncrNum;
		case "RANDOM":
			Random random = new Random();
			return random.nextLong();
		case "DATE()":
		case "DATE('NOW')":
			LocalDate currentDate = LocalDate.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			return currentDate.format(formatter);
		case "TIME()":
		case "TIME('NOW')":
			LocalTime currentTime = LocalTime.now();
			formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
			return currentTime.format(formatter);
		case "DATETIME()":
		case "DATETIME('NOW')":
		case "CURRENT_TIMESTAMP":
			LocalDateTime currentDateTime = LocalDateTime.now();
			formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			return currentDateTime.format(formatter);
		case "JULIANDAY()":
		case "JULIANDAY('now')":
			currentDateTime = LocalDateTime.now();
			int year = currentDateTime.getYear();
			int month = currentDateTime.getMonthValue();
			int day = currentDateTime.getDayOfMonth();
			int hour = currentDateTime.getHour();
			int minute = currentDateTime.getMinute();
			int second = currentDateTime.getSecond();

			int a = (14 - month) / 12;
			int y = year + 4800 - a;
			int m = month + 12 * a - 3;

			int julianDay = day + (153 * m + 2) / 5 + y * 365 + y / 4 - y / 100 + y / 400 - 32045;
			double julianTime = (hour - 12) / 24.0 + minute / 1440.0 + second / 86400.0;
			return julianDay + julianTime;
		}
		return null;
	}


	final static void validateInsertForDBLoggerAndAppender(String strippedSql) throws SQLException {

		// Create a Matcher object
		Matcher matcher = INSERT_DBLOGGER_APPENDER_PATTERN.matcher(strippedSql);

		// Check if the input string matches the pattern
		if (!matcher.matches()) {
			throw new SQLException("Unsupported Syntax for INSERT. Supported Syntaxes are: 1. INSERT INTO <dbName>.<tableName> (col1, col2, ...) VALUES (<literal|?>, ...) 2. INSERT INTO <tableName>(col1, col2, ...) VALUES(<literal|?>, ...) 3. INSERT INTO <tableName> VALUES(<literal|?>, ...). Function calls and subqueries are not permitted in the VALUES list.");
		}

		// If a column list is present, verify column count matches value count
		String columnList = matcher.group(2);
		if (columnList != null) {
			int colCount = columnList.split(",").length;
			int valCount = matcher.group(matcher.groupCount()).split(",").length;
			if (colCount != valCount) {
				throw new SQLException("INSERT column count (" + colCount + ") does not match value count (" + valCount + "). Each specified column must have a corresponding value.");
			}
		}
	}

	final static void validateInsertForDBLoggerAndAppender(String strippedSql, SQLiteConnection conn, SQLLogger logger) throws SQLException {
		// Step 1: syntax + column/value consistency (always)
		validateInsertForDBLoggerAndAppender(strippedSql);

		if (conn == null) return;

		// Step 2: extract table name
		Matcher tableNameMatcher = INSERT_TABLE_NAME_PATTERN.matcher(strippedSql);
		if (!tableNameMatcher.find()) return;
		String tableName = tableNameMatcher.group(1);

		// Step 3: lookup column count for this table (use logger cache when available,
		//         otherwise fall back to a direct PRAGMA query on the connection)
		int tableColCount;
		try {
			if (logger != null) {
				tableColCount = logger.getOrLoadTableColumnCount(tableName, conn);
			} else {
				tableColCount = 0;
				try (java.sql.Statement stmt = conn.createStatement();
					 java.sql.ResultSet rs = stmt.executeQuery("PRAGMA table_info(" + tableName.toUpperCase() + ")")) {
					while (rs.next()) tableColCount++;
				}
			}
		} catch (Exception e) {
			return; // schema not available yet — skip check
		}
		if (tableColCount == 0) return; // table not yet known — skip check

		// Step 4: compare counts
		Matcher insertMatcher = INSERT_DBLOGGER_APPENDER_PATTERN.matcher(strippedSql);
		if (!insertMatcher.matches()) return;
		String columnList = insertMatcher.group(2);
		if (columnList != null) {
			int colCount = columnList.split(",").length;
			if (colCount != tableColCount) {
				throw new SQLException("INSERT column count (" + colCount + ") does not match table '" + tableName + "' column count (" + tableColCount + "). All table columns must be specified.");
			}
		} else {
			int valCount = insertMatcher.group(insertMatcher.groupCount()).split(",").length;
			if (valCount != tableColCount) {
				throw new SQLException("INSERT value count (" + valCount + ") does not match table '" + tableName + "' column count (" + tableColCount + "). A value must be supplied for every table column.");
			}
		}
	}

	final static void validateUpdateForDBLoggerAndAppender(String strippedSql) throws SQLException {

		// Create a Matcher object
		Matcher matcher = UPDATE_DBLOGGER_APPENDER_PATTERN.matcher(strippedSql);

		// Check if the input string matches the pattern
		if (!matcher.matches()) {
			throw new SQLException("Unsupported Syntax for UPDATE. Supported Syntax is: UPDATE [<dbName>.]<tableName> SET col1 = <literal|?> [, col2 = <literal|?>] [WHERE col1 = <literal|?> [AND col2 = <literal|?>]]. Subqueries and function calls are not permitted.");
		}
	}

	final static void validateDeleteForDBLoggerAndAppender(String strippedSql) throws SQLException {

		// Create a Matcher object
		Matcher matcher = DELETE_DBLOGGER_APPENDER_PATTERN.matcher(strippedSql);

		// Check if the input string matches the pattern
		if (!matcher.matches()) {
			throw new SQLException("Unsupported Syntax for DELETE. Supported Syntax is: DELETE FROM [<dbName>.]<tableName> [WHERE col1 = <literal|?> [AND col2 = <literal|?>]]. Subqueries and function calls are not permitted.");
		}
	}

	/*
	final static boolean isTableDDL(String sql) {
		//Check if its a basic table DDL statement : CREATE or ALTER
		//This is specifically done for ALTER TABLE RENAME as it was not
		//throwing an exception that we expected in the catch block.
		//
		String strippedSql = sql.stripLeading();
		if ((strippedSql.regionMatches(true, 0, "CREATE", 0, 6)) ||
				(strippedSql.regionMatches(true, 0, "DROP", 0, 4)) ||
				(strippedSql.regionMatches(true, 0, "ALTER", 0, 5))) {
			return true;
		}
		return false;    	
	}
	*/

	final static String getTableNameFromDDL(String sql) {
		// Check if it's a basic table DDL statement: CREATE, DROP, or ALTER
		String strippedSql = sql.stripLeading().toUpperCase();
		String tableName = null;

        if (strippedSql.startsWith("CREATE") || strippedSql.startsWith("DROP") || strippedSql.startsWith("ALTER")) {
            Matcher createMatcher = DDL_CREATE_TABLE_PATTERN.matcher(strippedSql);
            Matcher dropMatcher   = DDL_DROP_TABLE_PATTERN.matcher(strippedSql);
            Matcher alterMatcher  = DDL_ALTER_TABLE_PATTERN.matcher(strippedSql);

            if (createMatcher.find()) {
                tableName = createMatcher.group(1);
            } else if (dropMatcher.find()) {
                tableName = dropMatcher.group(1);
            } else if (alterMatcher.find()) {
                tableName = alterMatcher.group(1);
            }
        }
		return tableName;
	}

	final static void validateProtectedInternalTableDDL(String sql, String tableName) throws SQLException {
		if (tableName == null) {
			return;
		}

		String strippedSql = sql.stripLeading();
		if (strippedSql.regionMatches(true, 0, "DROP", 0, 4) && PROTECTED_TXN_TABLE_NAME.equalsIgnoreCase(tableName)) {
			throw new SQLException("Protected internal table 'synclite_txn' cannot be dropped by user SQL.");
		}
	}


	final static boolean isDDLIdempotencyException(SQLException e) {
		String msg = e.getMessage();
		if (msg != null && (msg.contains("already exists") || msg.contains("no such table") || msg.contains("duplicate column name") || msg.contains("no such column"))) {
			return true;
		}
		return false;
	}

	final static boolean isTransactionalDevice(String strVal) {
		DeviceType deviceType = DeviceType.valueOf(strVal);
		switch (deviceType) {
		case SQLITE:
		case DUCKDB:
		case DERBY:
		case H2:
		case HYPERSQL:	
			return true;
		default:
			return false;
		}

	}

	static boolean deviceAllowsConcurrentWriters(DeviceType deviceType) {
		switch (deviceType) {
		case SQLITE:
		case SQLITE_APPENDER:
		case SQLITE_STORE:
			return false;
		case DUCKDB:
		case DUCKDB_APPENDER:
		case DUCKDB_STORE:
			return true;
		case DERBY:
		case DERBY_APPENDER:
		case DERBY_STORE:
			return true;
		case H2:
		case H2_APPENDER:
		case H2_STORE:
			return true;
		case HYPERSQL:
		case HYPERSQL_APPENDER:
		case HYPERSQL_STORE:
			return true;
		case DBLOGGER:
			return false;
		case STREAMING:
			return true;
		default:
			return false;
		}
	}
	
	static String getCreateTableSql(String tableName, HashMap<Integer, Column> tableInfo) {
		StringBuilder createTableSqlBuilder = new StringBuilder();
		boolean first = true;
		createTableSqlBuilder.append("CREATE TABLE IF NOT EXISTS " + tableName + "(");
		for (int i=0 ; i < tableInfo.size(); ++i) {
			Column c = tableInfo.get(i);
			String notNull = (c.isNullable == true) ? "" : "NOT NULL";
			if (!first) {
				createTableSqlBuilder.append(",");
			}
			String defaultValue = (c.defaultValue != null) ? "DEFAULT(" + c.defaultValue + ")" : "";
			String primaryKey = (c.isPrimaryKey == true) ? "PRIMARY KEY" : "";
					
			createTableSqlBuilder.append(c.name).append(" ").append(c.type).append(" ")
			.append(notNull).append(" ").append(defaultValue).append(" ").append(primaryKey);

			first = false;
		}
		createTableSqlBuilder.append(")");
		return createTableSqlBuilder.toString();
	}


	static String getDDLStatement(String tableName, HashMap<Integer, Column> beforeInfo, HashMap<Integer, Column> afterInfo, String sql) {
	    String strippedSql = sql.stripLeading().toUpperCase();
	    if (strippedSql.startsWith("CREATE")) {
	        // Just prepare a CREATE TABLE SQL and return it
	        String mappedSql = getCreateTableSql(tableName, afterInfo);
	        return mappedSql;
	    } else if (strippedSql.startsWith("DROP")) {
	        // Just prepare a DROP TABLE SQL and return it
	        String mappedSql = "DROP TABLE IF EXISTS " + tableName;
	        return mappedSql;
	    } else if (strippedSql.startsWith("ALTER")) {
	    	strippedSql = strippedSql.replaceFirst("^ALTER\\s+TABLE\\s+", "").trim();
	        // Handle ADD COLUMN
	        Matcher matcher = DDL_ADD_COLUMN_PATTERN.matcher(strippedSql);
	        if (matcher.find()) {
	            // This is an ADD COLUMN
	        	
	        	Set<String> beforeColNames = new HashSet<String>();
	            for (Map.Entry<Integer, Column> entry : beforeInfo.entrySet()) {
	            	beforeColNames.add(entry.getValue().name);
	            }

	            for (Map.Entry<Integer, Column> entry : afterInfo.entrySet()) {
	                String afterColName = entry.getValue().name;
	                Column afterColObj = entry.getValue();
	                if (!beforeColNames.contains(afterColName)) {
	                    // This is a new column.
	                    String mappedSql = "ALTER TABLE " + tableName + " ADD COLUMN " + afterColName + " " + afterColObj.type + " " + (afterColObj.isNullable ? "NULL" : "NOT NULL") + " " + ((afterColObj.defaultValue != null) ? "DEFAULT(" + afterColObj.defaultValue + ")" : "");
	                    return mappedSql;
	                }
	            }
	        } else {
	            // Handle DROP COLUMN
	            matcher = DDL_DROP_COLUMN_PATTERN.matcher(strippedSql);
	            if (matcher.find()) {
	                // This is a DROP COLUMN

		        	Set<String> afterColNames = new HashSet<String>();
		            for (Map.Entry<Integer, Column> entry : afterInfo.entrySet()) {
		            	afterColNames.add(entry.getValue().name);
		            }

	            	for (Map.Entry<Integer, Column> entry : beforeInfo.entrySet()) {
	                    String beforeColName = entry.getValue().name;
	                    if (!afterColNames.contains(beforeColName)) {
	                        // This is a dropped column.
	                        String mappedSql = "ALTER TABLE " + tableName + " DROP COLUMN " + beforeColName;
	                        return mappedSql;
	                    }
	                }
	            } else {
	                // Handle ALTER COLUMN
	                matcher = DDL_ALTER_COLUMN_PATTERN.matcher(strippedSql);
	                if (matcher.find()) {
	                	//Convert given maps to maps with column names as keys
	                	
	                	HashMap<String, Column> beforeColInfo = new HashMap<String, Column>();
	                	for (Map.Entry<Integer, Column> entry : beforeInfo.entrySet()) {
	                		beforeColInfo.put(entry.getValue().name, entry.getValue());
	                	}
	                	
	                	HashMap<String, Column> afterColInfo = new HashMap<String, Column>();
	                	for (Map.Entry<Integer, Column> entry : afterInfo.entrySet()) {
	                		afterColInfo.put(entry.getValue().name, entry.getValue());
	                	}
	                	
	                    for (Map.Entry<String, Column> entry : afterColInfo.entrySet()) {
	                        String afterColName = entry.getKey();
	                        Column afterColObj = entry.getValue();
	                        if (beforeColInfo.containsKey(afterColName)) {
	                            Column beforeColObj = beforeColInfo.get(afterColName);
	                            if (!beforeColObj.type.equals(afterColObj.type) || (beforeColObj.isNullable != afterColObj.isNullable) || (beforeColObj.defaultValue == null ? afterColObj.defaultValue != null : !beforeColObj.defaultValue.equals(afterColObj.defaultValue))) {
	                                // Some alteration found.
	                                String mappedSql = "ALTER TABLE " + tableName + " ALTER COLUMN " + afterColName + " " + afterColObj.type + " " + (afterColObj.isNullable ? "NULL" : "NOT NULL") + " " + ((afterColObj.defaultValue != null) ? "DEFAULT(" + afterColObj.defaultValue + ")" : "");
	                                return mappedSql;
	                            }
	                        }
	                    }
	                    // If we are here, we did not find any alteration.
	                    // Let the DDL go through, return empty string.
	                    return "";
	                } else {
	                    // Handle RENAME TO
	                    matcher = DDL_RENAME_TO_PATTERN.matcher(strippedSql);
	                    if (matcher.find()) {
                            String[] words = sql.split("\\s+");
	                        String newTableName = words[words.length -1];
	                        String mappedSql = "ALTER TABLE " + tableName + " RENAME TO " + newTableName;
	                        return mappedSql;
	                    } else {
	                        // Handle RENAME COLUMN
	                        matcher = DDL_RENAME_COLUMN_PATTERN.matcher(strippedSql);
	                        if (matcher.find()) {
	                            String[] words = sql.split("\\s+");
	                            if (words.length >= 3 && words[words.length - 2].equalsIgnoreCase("TO")) {
	                                String oldColumnName = words[words.length - 3];
	                                String newColumnName = words[words.length - 1];
	                                String mappedSql = "ALTER TABLE " + tableName + " RENAME COLUMN " + oldColumnName + " TO " + newColumnName;
	                                return mappedSql;
	                            }
	                        }
	                    }
	                }
	            }
	        }
	    }
	    return null;
	}

}
