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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class DerbyAppenderConnection extends MultiWriterDBAppenderConnection {		
    public static final String PREFIX = "jdbc:synclite_derby_appender:";
    private String derbyURL;
    protected Connection derbyConnection;

	public DerbyAppenderConnection(String url, String fileName, Properties props) throws SQLException {
		super(url, fileName, props);
	}	
	
    protected void prepareCommitLoggerPStmt() throws SQLException {
    	this.nativeCommitLoggerPStmt = derbyConnection.prepareStatement(insertCommitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT); 
    }

    SQLStager getCommandStager() {
    	return this.cmdStager;
    }
   
    @Override
	final protected void initDevice(Properties prop) throws SQLException {
    	Object configPathObj = prop.get("config");
    	Object deviceName = prop.get("device-name");
    	if (configPathObj != null) {
    		//Try initializing
    		if (deviceName != null) {
    			DerbyAppender.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
    		} else {
    			DerbyAppender.initialize(this.path, Path.of(configPathObj.toString()));
    		}
    	} else {
    		//Try initializing without configs.
    		if (deviceName != null) {
    			DerbyAppender.initialize(this.path, deviceName.toString());
    		} else {
    			DerbyAppender.initialize(this.path);
    		}
    	}
	}

    @Override
	protected void initDeviceWithoutProps() throws SQLException {
    	DerbyAppender.initialize(this.path);		
	}

    @Override
    final protected void doInitConn() throws SQLException  {
    	try {
    		this.derbyURL = "jdbc:derby:" + this.path.toString();
    		this.derbyConnection = DriverManager.getConnection(derbyURL, this.props);
    		connAutoCommit(false);
    	} catch (Exception e) {
    		throw new SQLException("Failed to connect to the derby database file : " + this.path + " : " + e.getMessage(), e);
    	}
    }

	@Override
	Connection getNativeDBConnection() {
		return this.derbyConnection;
	}

	@Override
	protected void nativeCommit() throws SQLException {
        if (derbyConnection != null) {
        	derbyConnection.commit();
        }
	}

	@Override
	protected void nativeSetAutoCommit(boolean b) throws SQLException {
        if (derbyConnection != null) {
        	derbyConnection.setAutoCommit(b);
        }
	}

	@Override
	protected void nativeRollback() throws SQLException {
        if (derbyConnection != null) {
        	derbyConnection.rollback();
        }
	}

	@Override
	protected PreparedStatement nativeUnloggedPreparedStatement(String sql) throws SQLException {
    	return derbyConnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	protected void nativeCloseCommitLoggerPStmt() throws SQLException {
		if (this.nativeCommitLoggerPStmt != null) {
			this.nativeCommitLoggerPStmt.close();
		}
	}

	@Override
	protected void nativeCloseConnection() throws SQLException {
    	if (this.derbyConnection != null) {
    		if (! this.derbyConnection.isClosed()) {
    			this.derbyConnection.rollback();
        		this.derbyConnection.close();
    		}
    	}
	}

	@Override 
	MultiWriterDBProcessor nativeDBProcessor() {
		return new DerbyProcessor();
	}
}
