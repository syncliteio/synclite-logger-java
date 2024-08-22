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

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4Statement;

public class SyncLiteStatement extends JDBC4Statement {
    private SQLLogger sqlLogger;

    protected SyncLiteStatement(SQLiteConnection conn) throws SQLException {
        super(conn);
        this.sqlLogger = AsyncTxnLogger.findInstance(getConn().getPath());
    }

    protected SyncLiteConnection getConn() {
        return ((SyncLiteConnection ) this.conn);
    }

    protected void log(String sql) throws SQLException {
        long commitId = getConn().getCommitId();
        sqlLogger.log(commitId, sql, null);
    }

    public final boolean executeUnlogged(String sql) throws SQLException {    	
        boolean result = stmtExecute(sql, SyncLiteUtils.getTableNameFromDDL(sql), new StringBuilder());
		if (getConn().getUserAutoCommit() == true) {
			getConn().connCommit();
		}
		return result;
    }

	@Override
    public final boolean execute(String sql) throws SQLException {
        boolean result = false;
        for (String nextSql : SyncLiteUtils.splitSqls(sql)) {
            if (!getConn().processIfTxnMessage(nextSql)) {
                result = executeInternal(nextSql);
            }
        }
        return result;
    }

    private final boolean executeInternal(String sql) throws SQLException {
    	boolean result = false;
		StringBuilder sqlToLog = new StringBuilder();
    	try {
    		result = stmtExecute(sql, SyncLiteUtils.getTableNameFromDDL(sql), sqlToLog);
    	} catch (SQLException e) {
    		if (e.getMessage().contains("syntax error") || e.getMessage().contains("Parse error")) {
    			try {
    				return SyncLiteUtils.checkAndExecuteInternalTransactionalSql(sql);
    			} catch (SQLException e1) {
    				if (e1.getMessage().contains("Unsupported SQL")) {
    					//Throw original exception
    					throw e;
    				}
    			}
    		} else {
    			throw e;
    		}
    	}
    	if (!sqlToLog.toString().isBlank()) {
    		log(sqlToLog.toString());
    	}
        processCommit();
        return result;
    }

    private final int executeUpdateInternal(String sql) throws SQLException {
    	int result = 0;
		StringBuilder sqlToLog = new StringBuilder();
    	try {
    		result = stmtExecuteUpdate(sql, SyncLiteUtils.getTableNameFromDDL(sql), sqlToLog);
    	} catch (SQLException e) {
    		if (e.getMessage().contains("syntax error") || e.getMessage().contains("Parse error")) {
    			try {
    				SyncLiteUtils.checkAndExecuteInternalTransactionalSql(sql);
    				return 1;
    			} catch (SQLException e1) {
    				if (e1.getMessage().contains("Unsupported SQL")) {
    					//Throw original exception
    					throw e;
    				}
    			}
    		} else {
    			throw e;
    		}
    	}
    	if (!sqlToLog.toString().isBlank()) {
    		log(sqlToLog.toString());
    	}
        processCommit();
        return result;
    }

    @Override
    public final ResultSet executeQuery(String sql) throws SQLException {
        ResultSet resultSet = null;
        for (String nextSql : SyncLiteUtils.splitSqls(sql)) {
            if (!getConn().processIfTxnMessage(nextSql)) {
                resultSet = executeQueryInternal(nextSql);
            }
        }
        return resultSet;
    }

    private final ResultSet executeQueryInternal(String sql) throws SQLException {
    	ResultSet rs = null;
        try {
        	String tableNameInDDL = SyncLiteUtils.getTableNameFromDDL(sql);
        	rs = stmtExecuteQuery(sql, tableNameInDDL);
            if (tableNameInDDL != null) {
                log(sql);
                processCommit();
            }
            return rs;
        } catch (SQLException e) {            
    		if (e.getMessage().contains("syntax error") || e.getMessage().contains("Parse error")) {
    			try {
    				SyncLiteUtils.checkAndExecuteInternalTransactionalSql(sql);
    				return null;
    			} catch (SQLException e1) {
    				if (e1.getMessage().contains("Unsupported SQL")) {
    					//Throw original exception
    					throw e;
    				}
    			}
    		} else if ((e.getSQLState() != null) && (e.getSQLState().equals("SQLITE_DONE"))) {
                //Possibly a write operation executed using executeQuery, log it
                log(sql);
                processCommit();
            } else {
            	throw e;
            }
        }
        return rs;
    }

    @Override
    public final int executeUpdate(String sql) throws SQLException {
        int result = 0;
        for (String nextSql : SyncLiteUtils.splitSqls(sql)) {
            if (!getConn().processIfTxnMessage(nextSql)) {
                result = executeUpdateInternal(nextSql);
            }
        }
        return result;
    }

    public final int executeUnloggedUpdate(String sql) throws SQLException {    	
        return stmtExecuteUpdate(sql, SyncLiteUtils.getTableNameFromDDL(sql), new StringBuilder());
    }

    private final void processCommit() throws SQLException {
        if (getConn().getUserAutoCommit() == true) {
            getConn().commit();
        }
    }

    protected boolean stmtExecute(String sql, String tableNameInDDL, StringBuilder sqlToLog) throws SQLException {
    	boolean res=superExecute(sql);
    	sqlToLog.append(sql);
    	return res;
    }

    protected SyncLiteStatement getSyncLiteStatement() {
    	return this;
    }
    
    final boolean superExecute(String sql) throws SQLException {
    	return super.execute(sql);
    }    
    
    protected ResultSet stmtExecuteQuery(String sql, String tableNameInDDL) throws SQLException {
    	return super.executeQuery(sql);
    }

    protected int stmtExecuteUpdate(String sql, String tableNameInDDL, StringBuilder sqlToLog) throws SQLException {    	
    	int res=super.executeUpdate(sql);
    	sqlToLog.append(sql);
    	return res;
    }
    
    @Override
    public void close() throws SQLException {
        super.close();
    }
}
