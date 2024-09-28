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

public class TelemetryConnection extends JDBC4Connection {

    public static final String PREFIX = "jdbc:synclite_telemetry:";
    public static final String commitLoggerSql = "UPDATE synclite_txn SET commit_id = ?, operation_id = ?";
    private boolean userAutoCommit;
    private PreparedStatement commitLoggerPstmt;
    protected Path path;
    protected long commitId;
    protected EventLogger sqlLogger;
    private boolean ready = false;
    private Properties props;
    public TelemetryConnection(String url, String fileName, Properties prop) throws SQLException {
        super(url, fileName, prop);
        this.path = Path.of(fileName);
        this.userAutoCommit = true;
        this.props = prop;
        super.setAutoCommit(false);
        this.sqlLogger = (EventLogger) SQLLogger.findInstance(path);
        if (this.sqlLogger == null) {
        	//Check if props are specified and props have a property "config" with value as a path to a synclite logger configuration file.
        	if (prop != null) {
        		initDevice(prop);
        		cleanUpProps();
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

	protected void initDevice(Properties prop) throws SQLException {
    	Object configPathObj = prop.get("config");
    	Object deviceName = prop.get("device-name");
    	if (configPathObj != null) {
    		//Try initializing
    		if (deviceName != null) {
    			Telemetry.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
    		} else {
    			Telemetry.initialize(this.path, Path.of(configPathObj.toString()));
    		}	        		
    	} else {
    		//Try initializing without configs.
    		if (deviceName != null) {
    			Telemetry.initialize(this.path, deviceName.toString());
    		} else {
    			Telemetry.initialize(this.path);
    		}
    	}
	}

	protected void initDeviceWithoutProps() throws SQLException {
		Telemetry.initialize(this.path);
	}
	
	protected void doInitConn() throws SQLException {
		super.setAutoCommit(false);
	}

	
    protected void prepareCommitLoggerPStmt() throws SQLException {
        this.commitLoggerPstmt = prepareUnloggedStatement(commitLoggerSql);
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

    private final PreparedStatement prepareUnloggedStatement(String sql) throws SQLException {
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

    protected Statement connCreateStatement() throws SQLException {
    	return new TelemetryStatement(this);
    }

    protected PreparedStatement connPrepareStatement(String sql) throws SQLException {
    	return new TelemetryPreparedStatement(this, sql);
    }

    public final PreparedStatement prepareUnloggedStatement(String sql, int rst, int rsc, int rsh) throws SQLException {
		return super.prepareStatement(sql, rst, rsc, rsh);    	    	
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
        	return connPrepareStatement(sql);
        } catch (SQLException e) {
        	if (e.getMessage().contains("syntax error") || e.getMessage().contains("Parse error")) {
        		//This may be an internal internal SQL supported by SyncLite but not by SQLite
        		try {
        			pstmt = new InternalTelemetryPreparedStatement(this, sql);
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

    @Override 
    public void commit() throws SQLException {
        recordCommit();
        //
        //2 PC
        //Flush the log in log database
        //Commit on the master database
        //
    	this.sqlLogger.commit(commitId);
        super.commit();
        this.commitId = this.sqlLogger.getNextCommitID();
    }

    @Override
	public void rollback() throws SQLException {
    	sqlLogger.rollback(commitId);
    	super.rollback();
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

	protected final void superRollback() throws SQLException {
		super.rollback();
	}

}
