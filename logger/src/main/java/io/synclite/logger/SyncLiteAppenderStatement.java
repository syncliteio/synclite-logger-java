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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4Statement;

public class SyncLiteAppenderStatement extends JDBC4Statement {
    private SQLLogger sqlLogger;
    protected SyncLiteAppenderStatement(SQLiteConnection conn) throws SQLException {
        super(conn);
        this.sqlLogger = EventLogger.findInstance(getConn().getPath());
    }

    protected SyncLiteAppenderConnection getConn() {
        return ((SyncLiteAppenderConnection ) this.conn);
    }

    public void log(String sql) throws SQLException {
        long commitId = getConn().getCommitId();
        sqlLogger.log(commitId, sql, null);
    }

	public final boolean executeUnlogged(String sql) throws SQLException {
		boolean result = stmtExecute(sql, SyncLiteUtils.getTableNameFromDDL(sql), new StringBuilder());
		if (getConn().getUserAutoCommit() == true) {
			getConn().superCommit();
		}
		return result;
	}

    @Override
    public final boolean execute(String sql) throws SQLException {
        boolean result = false;
        List<String> sqls = SyncLiteUtils.splitSqls(sql);
        for (int i=0; i<sqls.size();++i) {
        	result = executeSingleSQL(sqls.get(i));
        }
        return result;
    }

    private final boolean executeSingleSQL(String sql) throws SQLException {
    	boolean result = false;
    	String strippedSql = sql.strip();
    	String tokens[] = strippedSql.split("\\s+");
    	if (tokens[0].equalsIgnoreCase("INSERT") && tokens[1].equalsIgnoreCase("INTO")) {
    		SyncLiteUtils.validateInsertForTelemetryAndAppender(strippedSql);
    		result = executeInsert(sql);
    	} else if ((tokens[0].equalsIgnoreCase("CREATE") || tokens[0].equalsIgnoreCase("DROP") || tokens[0].equalsIgnoreCase("ALTER")) &&
    			(tokens[1].equalsIgnoreCase("TABLE"))
    			) {
    		result = executeDDL(sql);
    	} else if ((tokens.length == 4) && tokens[0].equalsIgnoreCase("PUBLISH") && tokens[1].equalsIgnoreCase("COLUMN") && tokens[2].equalsIgnoreCase("LIST")) {	
			result = executePublishColumnList(sql, tokens);
    	}  else if (tokens[0].equalsIgnoreCase("DELETE") || tokens[0].equalsIgnoreCase("UPDATE")) {
			throw new SQLException("Unsupported SQL : SyncLite appender device does not support SQL : " + sql + ". Supported SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, SELECT");
		}  else {
			try {
				SyncLiteUtils.checkAndExecuteInternalAppenderSql(sql);
			} catch (SQLException e) {
				throw new SQLException("Unsupported SQL : SyncLite appender device does not support SQL : " + sql + ". Supported SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, SELECT");
			}
    	}
    	return result;
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

    protected boolean stmtExecute(String sql, String tableNameInDDL, StringBuilder sqlToLog) throws SQLException {
    	boolean res=superExecute(sql);
    	sqlToLog.append(sql);
    	return res;
    }

    final boolean superExecute(String sql) throws SQLException {
    	return super.execute(sql);
    }

	private final boolean executeDDL(String sql) throws SQLException {
		StringBuilder sqlToLog = new StringBuilder();		
        boolean result = stmtExecute(sql, SyncLiteUtils.getTableNameFromDDL(sql), sqlToLog);
        log(sqlToLog.toString());
        processCommit();
        return result;
    }
    
    private final boolean executeInsert(String sql) throws SQLException {
		StringBuilder sqlToLog = new StringBuilder();		
    	boolean result = stmtExecute(sql, SyncLiteUtils.getTableNameFromDDL(sql), sqlToLog);
    	log(sqlToLog.toString());
    	processCommit();
    	return result;
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
						rs = stmtExecuteQuery(sql, SyncLiteUtils.getTableNameFromDDL(sql));
					} else {
						throw e;
					}
				}
			}
		}
		return rs;
    }

    protected ResultSet stmtExecuteQuery(String sql, String tableNameInDDL) throws SQLException {
    	return super.executeQuery(sql);
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
    
    @Override
    public void close() throws SQLException {
        super.close();
    }

}
