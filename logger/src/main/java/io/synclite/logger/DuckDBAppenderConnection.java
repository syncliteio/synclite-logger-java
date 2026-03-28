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

public class DuckDBAppenderConnection extends MultiWriterDBAppenderConnection {
    public static final String PREFIX = "jdbc:synclite_duckdb_appender:";
    private String duckDBURL;
    protected Connection duckDBConnection;

	public DuckDBAppenderConnection(String url, String fileName, Properties props) throws SQLException {
    	super(url, fileName, props);
	}	
	
    protected void prepareCommitLoggerPStmt() throws SQLException {
    	this.nativeCommitLoggerPStmt = duckDBConnection.prepareStatement(insertCommitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }
   
    @Override
	final protected void initDevice(Properties prop) throws SQLException {
    	Object configPathObj = prop.get("config");
    	Object deviceName = prop.get("device-name");
    	if (configPathObj != null) {
    		//Try initializing
    		if (deviceName != null) {
    			DuckDBAppender.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
    		} else {
    			DuckDBAppender.initialize(this.path, Path.of(configPathObj.toString()));
    		}
    	} else {
    		//Try initializing without configs.
    		if (deviceName != null) {
    			DuckDBAppender.initialize(this.path, deviceName.toString());
    		} else {
    			DuckDBAppender.initialize(this.path);
    		}
    	}
	}
    
    @Override
	protected void initDeviceWithoutProps() throws SQLException {
    	DuckDBAppender.initialize(this.path);		
	}


    @Override
    final protected void doInitConn() throws SQLException  {
    	try {
    		this.duckDBURL = "jdbc:duckdb:" + this.path.toString();
    		this.duckDBConnection = DriverManager.getConnection(duckDBURL, this.props);
    		connAutoCommit(false);
    	} catch (Exception e) {
    		throw new SQLException("Failed to connect to the duckdb database file : " + this.path + " : " + e.getMessage(), e);
    	}
    }
	

	@Override
	protected void nativeCommit() throws SQLException {
		if (this.duckDBConnection != null) {
			this.duckDBConnection.commit();
		}
	}

	@Override
	protected void nativeSetAutoCommit(boolean b) throws SQLException {
		if (this.duckDBConnection != null) {
			this.duckDBConnection.setAutoCommit(b);
		}		
	}

	@Override
	protected void nativeRollback() throws SQLException {
		if (this.duckDBConnection != null) {
			this.duckDBConnection.rollback();
		}
	}

	@Override
	protected PreparedStatement nativeUnloggedPreparedStatement(String sql) throws SQLException {
    	return this.duckDBConnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	protected void nativeCloseConnection() throws SQLException {
		if (this.duckDBConnection != null) {
			this.duckDBConnection.close();
		}
	}
	
	@Override
	protected void nativeCloseCommitLoggerPStmt() throws SQLException {
		if (this.nativeCommitLoggerPStmt != null) {
			this.nativeCommitLoggerPStmt.close();
		}
	}

	@Override
	MultiWriterDBProcessor nativeDBProcessor() {
		return new DuckDBProcessor();
	}

	@Override
	Connection getNativeDBConnection() {
		return this.duckDBConnection;
	}	

}
