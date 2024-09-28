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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.sqlite.SQLiteConnection;

import io.synclite.logger.MultiWriterDBProcessor.Column;

public class MultiWriterDBAppenderStatement extends SyncLiteAppenderStatement {

	private final Statement stmt;
    private static final Object metadataDDLLock = new Object();

	protected MultiWriterDBAppenderStatement(SQLiteConnection conn) throws SQLException {
		super(conn);
		stmt = getConn().getNativeDBConnection().createStatement();
	}

	@Override
    final protected MultiWriterDBAppenderConnection getConn() {
        return ((MultiWriterDBAppenderConnection ) this.conn);
    }
    
	final protected boolean stmtExecute(String sql, String tableNameInDDL, StringBuilder sqlToLog) throws SQLException {
		HashMap<Integer, Column> beforeInfo = null;
		HashMap<Integer, Column> afterInfo = null;
		if (tableNameInDDL != null) {
			beforeInfo = getConn().getDBProcessor().getSrcTableInfo(tableNameInDDL, getConn().getNativeDBConnection());
		} else {
			//validate the SQL on sqlite metadata file if it is a valid SQLite syntax.
			try (PreparedStatement pstmt = getConn().validateSQL(sql)) {
				//
			} catch (SQLException e) {
				throw new SQLException("Unsupporred SQL : " + sql + " : " + e.getMessage(), e);
			}
		}
    	boolean res = stmt.execute(sql);
		if (tableNameInDDL != null) {
			afterInfo = getConn().getDBProcessor().getSrcTableInfo(tableNameInDDL, getConn().getNativeDBConnection());
		}
		if (tableNameInDDL != null) {
			String mappedSql = SyncLiteUtils.getDDLStatement(tableNameInDDL, beforeInfo, afterInfo, sql);
	    	sqlToLog.append(mappedSql);
			if (sql == null) {
				throw new SQLException("Unsupported SQL : " + sql);
			} else {
				//
				//Simply execute drop and create table fresh on metadata file to bring it in sync
				//
				synchronized(metadataDDLLock) {
					if (mappedSql.contains("RENAME TO")) {
						//Just execute rename DDL
						super.superExecute(mappedSql);
					} else if (mappedSql.contains("DROP TABLE")) { 
						super.superExecute(mappedSql);
					}  else {
						String dropSql = "DROP TABLE IF EXISTS " + tableNameInDDL;				
						super.superExecute(dropSql);
						String createSql = SyncLiteUtils.getCreateTableSql(tableNameInDDL, afterInfo);				
						super.superExecute(createSql);
					}
				}
			}
		} else {
	    	sqlToLog.append(sql);
		}
    	return res;
    }

    final protected ResultSet stmtExecuteQuery(String sql, String tableNameInDDL) throws SQLException {
		if (tableNameInDDL != null) {
			throw new SQLException("DDL statements not permitted with executeQuery function");
		}    	
		ResultSet rs = stmt.executeQuery(sql);		
    	return rs;
    }

    final protected int stmtExecuteUpdate(String sql, String tableNameInDDL, StringBuilder sqlToLog) throws SQLException {
		HashMap<Integer, Column> beforeInfo = null;
		HashMap<Integer, Column> afterInfo = null;
		if (tableNameInDDL != null) {
			beforeInfo = getConn().getDBProcessor().getSrcTableInfo(tableNameInDDL, getConn().getNativeDBConnection());
		} else {
			//validate the SQL on sqlite metadata file if it is a valid SQLite syntax.
			try (PreparedStatement pstmt = getConn().validateSQL(sql)) {
				//
			} catch (SQLException e) {
				throw new SQLException("Unsupporred SQL : " + sql + " : " + e.getMessage(), e);
			}
		}

    	int res = stmt.executeUpdate(sql);

    	if (tableNameInDDL != null) {
			afterInfo = getConn().getDBProcessor().getSrcTableInfo(tableNameInDDL, getConn().getNativeDBConnection());
		}
		if (tableNameInDDL != null) {
			String mappedSql = SyncLiteUtils.getDDLStatement(tableNameInDDL, beforeInfo, afterInfo, sql);
	    	sqlToLog.append(mappedSql);
	    	//Execute this sql on sqlite metadata table as well.
			if (sql == null) {
				throw new SQLException("Unsupported SQL : " + sql);
			} else {
				//
				//Simply execute drop and create table fresh on metadata file to bring it in sync
				//
				synchronized(metadataDDLLock) {
					if (mappedSql.contains("RENAME TO")) {
						//Just execute rename DDL
						super.superExecute(mappedSql);
					} else if (mappedSql.contains("DROP TABLE")) { 
						super.superExecute(mappedSql);
					} else {
						String dropSql = "DROP TABLE IF EXISTS " + tableNameInDDL;				
						super.superExecute(dropSql);
						String createSql = SyncLiteUtils.getCreateTableSql(tableNameInDDL, afterInfo);				
						super.superExecute(createSql);
					}
				}				
			}
		} else {
	    	sqlToLog.append(sql);
		}
		
    	return res;
    }
    
    @Override
    final public void close() throws SQLException {
        super.close();
        if (this.stmt != null) {
        	this.stmt.close();
        }
    }

    private SQLStager getCommandStager() {
    	return getConn().getCommandStager();
    }

    @Override
	public final void log(String sql) throws SQLException {
        long commitId = getConn().getCommitId();
        getCommandStager().log(commitId, sql, null);
    }
}
