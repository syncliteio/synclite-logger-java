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
import java.util.Properties;

import org.apache.log4j.Logger;
import org.sqlite.SQLiteConnection;

public class SQLiteAppender extends SyncLite  {
	private final static String PREFIX = "jdbc:synclite_sqlite_appender:";
	
    @Override
	protected final SQLiteConnection createSyncLiteConnection(String url, Properties prop) throws SQLException {
        if (!checkDeviceURL(url)) {
            return null;       
        }
        url = url.trim();
        prop.put("original-db-url", url);
        prop.put("original-db-path", extractAddress(url, PREFIX));
        //return new DuckDBConnection(url, extractAddress(url), prop);
    	return new SyncLiteAppenderConnection(url, extractAddress(url, PREFIX), prop); 
    }
    
    
	public static synchronized final void initialize(Path dbPath) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE_APPENDER, dbPath);
	}

	public static synchronized final void initialize(Path dbPath, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE_APPENDER, dbPath, deviceName);
	}

	public static synchronized final void initialize(Path dbPath, SyncLiteOptions options) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE_APPENDER, dbPath, options);
	}

	public static synchronized final void initialize(Path dbPath, SyncLiteOptions options, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE_APPENDER, dbPath, options, deviceName);
	}

	public static synchronized final void initialize(Path dbPath, Path propsPath) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE_APPENDER, dbPath, propsPath);
	}

	public static synchronized final void initialize(Path dbPath, Path propsPath, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE_APPENDER, dbPath, propsPath, deviceName);
	}
    
    @Override
    protected String getPrefix() {
    	return PREFIX;
    }

	protected DBProcessor getDBProcessor() {
		return new SQLiteProcessor();
	}	

	@Override
	protected void validateLibs(Logger tracer) throws SQLException {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			tracer.error("Failed to load sqlite jdbc driver : " + e.getMessage());
			throw new SQLException("Failed to load sqlite jdbc driver");
		}	
	}

	@Override
	protected void setDeviceTypeInOptions(SyncLiteOptions options) throws SQLException {
		options.SetDeviceType(DeviceType.SQLITE_APPENDER);
	}

	@Override
	protected void getOrCreateLoggerInstace(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		//Use SyncEventLogger for this device.
		SyncEventLogger.getInstance(dbPath, options, tracer);
	}
}
