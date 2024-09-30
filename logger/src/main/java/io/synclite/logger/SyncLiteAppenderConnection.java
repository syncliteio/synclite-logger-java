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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.sqlite.SQLiteException;
import org.sqlite.jdbc4.JDBC4Connection;

public class SyncLiteAppenderConnection extends JDBC4Connection {
	
    public static final String PREFIX = "jdbc:synclite_sqlite_appender:";
    public static final String updateCommitLoggerSql = "UPDATE synclite_txn SET commit_id = ?, operation_id = ?";
    private boolean userAutoCommit;
    private PreparedStatement commitLoggerPstmt;
    protected Path path;
    protected long commitId;
    protected EventLogger sqlLogger;
    private boolean ready = false;
    protected Properties props;
    public SyncLiteAppenderConnection(String url, String fileName, Properties prop) throws SQLException {
        super(url, fileName, prop);        
        this.props = prop;
        initPath(fileName);
        this.userAutoCommit = true;
        this.sqlLogger = (EventLogger) SQLLogger.findInstance(path);
        if (this.sqlLogger == null) {
        	//Check if props are specified and props have a property "config" with value as a path to a synclite logger configuration file.
        	//Check if props are specified and props have a property "config" with value as a path to a synclite logger configuration file.
        	if (prop != null) {
        		initDevice(prop);
        		this.sqlLogger = (EventLogger) SQLLogger.findInstance(path);
        	} else {
        		//Try initializing without configs.
        		initDeviceWithoutProps();
        		this.sqlLogger = (EventLogger) SQLLogger.findInstance(path);        		
        	}
        	if (this.sqlLogger == null) {
        		throw new SQLException("SyncLite device at path " + path + " not initialized. Please initialize the device first.");
        	}
        } else {
        	if (!this.sqlLogger.isLoggerHealthy()) {
        		throw new SQLException("SyncLite logger is not healthy for device : " + path + ". Please check device trace file for more details. Please close and initialize the device again.");
        	}
        }
        if (this.props != null) {
        	cleanUpProps();
        }
        initConn();
        this.commitId = this.sqlLogger.getNextCommitID();
        this.ready = true;
    }

    private final void cleanUpProps() {
    	//Remove SyncLite specific props from the props object
    	props.remove("config");
    	props.remove("device-name");
    }

	final void initConn() throws SQLException {
		doInitConn();
		prepareCommitLoggerPStmt();
	}
	
    protected void prepareCommitLoggerPStmt() throws SQLException {
    	this.commitLoggerPstmt = super.prepareStatement(updateCommitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT); 
    }

	protected void initDevice(Properties prop) throws SQLException {
    	Object configPathObj = prop.get("config");
    	Object deviceName = prop.get("device-name");
    	if (configPathObj != null) {
    		//Try initializing
    		if (deviceName != null) {
    			SQLite.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
    		} else {
    			SQLite.initialize(this.path, Path.of(configPathObj.toString()));
    		}
    	} else {
    		//Try initializing without configs.
    		if (deviceName != null) {
    			SQLite.initialize(this.path, deviceName.toString());
    		} else {
    			SQLite.initialize(this.path);
    		}
    	}
	}

	protected void initDeviceWithoutProps() throws SQLException {
		SQLite.initialize(this.path);		
	}
	
	protected void doInitConn() throws SQLException {
        connAutoCommit(false);
	}

    protected void connAutoCommit(boolean b) throws SQLException {
        super.setAutoCommit(b);
	}

    protected Statement connCreateStatement() throws SQLException {
    	return new SyncLiteAppenderStatement(this);
    }
    
    protected PreparedStatement connPrepareStatement(SyncLiteAppenderConnection conn, String sql) throws SQLException {
    	return new SyncLiteAppenderPreparedStatement(this, sql);
    }

    protected void connCommit() throws SQLException {
		super.commit();
	}

    protected void connRollback() throws SQLException {
        super.rollback();
	}

    protected void initPath(String fileName) {
		this.path = Path.of(fileName);		
	}

    final Path getPath() {
        return this.path;
    }

    final long getCommitId() {
        return commitId;
    }

    final long getOperationId() {
        return this.sqlLogger.getOperationID();
    }

    PreparedStatement prepareUnloggedStatement(String sql) throws SQLException {
        return super.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public final Statement createStatement(int rst, int rsc, int rsh) throws SQLException {
    	if (!this.ready) {
    		//
    		//If connection is not ready yet and this is called ( which is possible if super
    		//prepares statements) then continue creating super preparedstatements
    		//
    		return super.createStatement(rst, rsc, rsh);
    	}
        checkOpen();
        checkCursor(rst, rsc, rsh);
        return connCreateStatement();
    }

    @Override
    public final PreparedStatement prepareStatement(String sql, int rst, int rsc, int rsh) throws SQLException {
    	if (!this.ready) {
    		//
    		//If connection is not ready yet and this is called ( which is possible if super
    		//prepares statements) then continue creating super preparedstatements
    		//
    		return super.prepareStatement(sql, rst, rsc, rsh);
    	}
        checkOpen();
        checkCursor(rst, rsc, rsh);
        PreparedStatement pstmt = null;
        try {
        	return connPrepareStatement(this, sql);
        } catch (SQLException e) {
        	if (e.getMessage().contains("Unsupported SQL") || e.getMessage().contains("Parse error") || e.getMessage().contains("Unsupported SQL")) {
        		//This may be an internal internal SQL supported by SyncLite but not by SQLite
        		try {
        			pstmt = new InternalAppenderPreparedStatement(this, sql);
        		} catch (SQLiteException e1) {
        			if (e1.getMessage().contains("Unsupported SQL")) {
        				//throw original exception
        				throw e;
        			}
        		}
        	} else {
        		throw e;
        	}
        }
        return pstmt;
    }

    /*
	private final String validateAndRewriteSQL(String sql, HashMap<Integer, Object> specialPositionalArgs) throws SQLException {
		List<String> subSqls = SyncLiteUtils.splitSqls(sql);
		if (subSqls.size() > 1) {
			throw new SQLException("SyncLite Appender : Supports a single SQL statement as part of a PreparedStatement, multiple specified  : " + sql);			
		}
		
		String strippedSql = subSqls.get(0).strip();
		String[] tokens = strippedSql.split("\\s+");
		if ((tokens.length >= 4) && tokens[0].equalsIgnoreCase("INSERT")) {
    		SyncLiteUtils.validateInsertForTelemetryAndAppender(strippedSql);
			String rewrittenSQL = SyncLiteUtils.parsePositionalArgs(sql, specialPositionalArgs);
			if (rewrittenSQL != null) {
				return rewrittenSQL;
			}

		} else if ((tokens[0].equalsIgnoreCase("CREATE") || tokens[0].equalsIgnoreCase("DROP") || tokens[0].equalsIgnoreCase("ALTER")) &&
				(tokens[1].equalsIgnoreCase("TABLE"))
				) {
				//Nothing to do
		} else {
			throw new SQLException("SyncLite Appender : Unsupported SQL : " + sql);
		}		
		return sql;
	}
     */

    //TODO
    //Why are these public ??
    
    @Override 
    public void commit() throws SQLException {
        recordCommit();
        //Flush the log in log database
        //Commit on the master database
        //
    	this.sqlLogger.commit(commitId);
    	connCommit();
    	this.commitId = this.sqlLogger.getNextCommitID();
    }

    @Override
	public void rollback() throws SQLException {
    	sqlLogger.rollback(commitId);
    	connRollback();
    }

    protected void recordCommit() throws SQLException {
        commitLoggerPstmt.setLong(1, commitId);
        commitLoggerPstmt.setLong(2, this.sqlLogger.getOperationID());
        commitLoggerPstmt.execute();
    }

    final PreparedStatement getPstmt() {
        return commitLoggerPstmt;
    }

    @Override
    public final void setAutoCommit(boolean ac) throws SQLException {
        this.userAutoCommit = ac;
    }

    final boolean getUserAutoCommit() {
        return userAutoCommit;
    }

	protected final void superCommit() throws SQLException {
		super.commit();		
	}

    protected PreparedStatement validateSQL(String sql) throws SQLException {
   		return super.prepareStatement(sql);
    }

}
