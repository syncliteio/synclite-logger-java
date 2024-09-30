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

public class H2AppenderConnection extends MultiWriterDBAppenderConnection {		
    public static final String PREFIX = "jdbc:synclite_h2_appender:";
    private String h2URL;
    protected Connection h2Connection;

	public H2AppenderConnection(String url, String fileName, Properties props) throws SQLException {
		super(url, fileName, props);
	}	
	
    protected void prepareCommitLoggerPStmt() throws SQLException {
    	//this.nativeCommitLoggerPStmt = h2Connection.prepareStatement(commitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    	this.nativeCommitLoggerPStmt = h2Connection.prepareStatement(insertCommitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
	final protected void initDevice(Properties prop) throws SQLException {
    	Object configPathObj = prop.get("config");
    	Object deviceName = prop.get("device-name");
    	if (configPathObj != null) {
    		//Try initializing
    		if (deviceName != null) {
    			H2Appender.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
    		} else {
    			H2Appender.initialize(this.path, Path.of(configPathObj.toString()));
    		}
    	} else {
    		//Try initializing without configs.
    		if (deviceName != null) {
    			H2Appender.initialize(this.path, deviceName.toString());
    		} else {
    			H2Appender.initialize(this.path);
    		}
    	}
	}
    
    @Override
	protected void initDeviceWithoutProps() throws SQLException {
    	H2Appender.initialize(this.path);		
	}

    @Override
    final protected void doInitConn() throws SQLException  {
    	try {
    		this.h2URL = "jdbc:h2:" + this.path.toString();
    		this.h2Connection = DriverManager.getConnection(h2URL, this.props);
    		connAutoCommit(false);
    	} catch (Exception e) {
    		throw new SQLException("Failed to connect to the h2 database file : " + this.path + " : " + e.getMessage(), e);
    	}
    }

	final Connection getH2Connection() {
		return this.h2Connection;
	}
	
	@Override
	Connection getNativeDBConnection() {
		return this.h2Connection;
	}

	@Override
	protected void nativeCommit() throws SQLException {
        if (h2Connection != null) {
        	h2Connection.commit();
        }
	}

	@Override
	protected void nativeSetAutoCommit(boolean b) throws SQLException {
        if (h2Connection != null) {
        	h2Connection.setAutoCommit(b);
        }
	}

	@Override
	protected void nativeRollback() throws SQLException {
        if (h2Connection != null) {
        	h2Connection.rollback();
        }
	}

	@Override
	protected PreparedStatement nativeUnloggedPreparedStatement(String sql) throws SQLException {
    	return h2Connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	protected void nativeCloseCommitLoggerPStmt() throws SQLException {
		if (this.nativeCommitLoggerPStmt != null) {
			this.nativeCommitLoggerPStmt.close();
		}
	}

	@Override
	protected void nativeCloseConnection() throws SQLException {
    	if (this.h2Connection != null) {
    		if (! this.h2Connection.isClosed()) {
    			this.h2Connection.close();
    		}
    	}
	}

	@Override
	protected MultiWriterDBProcessor nativeDBProcessor() {
		return new H2Processor();
	}
}
