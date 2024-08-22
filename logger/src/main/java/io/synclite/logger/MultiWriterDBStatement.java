package io.synclite.logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.sqlite.SQLiteConnection;

import io.synclite.logger.MultiWriterDBProcessor.Column;

public class MultiWriterDBStatement extends SyncLiteStatement {

	private final Statement stmt;
    private static final Object metadataDDLLock = new Object();

	protected MultiWriterDBStatement(SQLiteConnection conn) throws SQLException {
		super(conn);
		stmt = getConn().getNativeDBConnection().createStatement();
	}

	@Override
    final protected MultiWriterDBConnection getConn() {
        return ((MultiWriterDBConnection ) this.conn);
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
	protected final void log(String sql) throws SQLException {
        long commitId = getConn().getCommitId();
        getCommandStager().log(commitId, sql, null);
    }
}
