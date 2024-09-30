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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.sqlite.SQLiteConnection;

public class SyncLite extends org.sqlite.JDBC {

	private final static String PREFIX = "jdbc:synclite";

	private static HashMap<DeviceType, SyncLite> INSTANCES_BY_DEVICE_TYPES = new HashMap<DeviceType, SyncLite>();
	private static HashMap<String, SyncLite> INSTANCES_BY_PRREFIXES = new HashMap<String, SyncLite>();
	private static ConcurrentHashMap<Path, Object> dbInitializationLocks = new ConcurrentHashMap<>();

	static
	{
		SyncLite instance;
		//Load embedded db drivers
		try {
			Class.forName("org.sqlite.JDBC");
    		Class.forName("org.duckdb.DuckDBDriver");
    		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    		Class.forName("org.h2.Driver");
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
		} catch (Exception e) {
			throw new RuntimeException("Failed to load JDBC driver(s) : " + e.getMessage(), e);
		}
		
		instance = new SQLite();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.SQLITE, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_sqlite:", instance);

		instance = new SQLiteAppender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.SQLITE_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_sqlite_appender:", instance);

		instance = new DuckDB();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DUCKDB, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_duckdb:", instance);

		instance = new DuckDBAppender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DUCKDB_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_duckdb_appender:", instance);

		instance = new Derby();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DERBY, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_derby:", instance);

		instance = new DerbyAppender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DERBY_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_derby_appender:", instance);

		instance = new H2();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.H2, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_h2:", instance);

		instance = new H2Appender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.H2_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_h2_appender:", instance);

		instance = new HyperSQL();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.HYPERSQL, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_hsqldb:", instance);

		instance = new HyperSQLAppender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.HYPERSQL_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_hsqldb_appender:", instance);

		instance = new Telemetry();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.TELEMETRY, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_telemetry:", instance);

		instance = new Streaming();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.STREAMING, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_streaming:", instance);

		try {
			DriverManager.registerDriver(new SyncLite());
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to register SyncLite JDBC driver : " + e.getMessage(), e);
		} 
	}


	protected SyncLite() {	}

	public static boolean isValidURL(String url) {
		return url != null && url.toLowerCase().startsWith(PREFIX);
	}

	protected boolean checkDeviceURL(String url) {
		return url != null && url.toLowerCase().startsWith(getPrefix());
	}

	public Connection connect(String url, Properties info) throws SQLException {
		return createConnection(url, info);
	}

	public static SQLiteConnection createConnection(String url, Properties prop) throws SQLException {
		url = url.trim();
		String[] tokens = url.split(":");
		if (tokens.length <= 2) {
			return null;
		}        
		String prefix = tokens[0] + ":" + tokens[1] + ":";        
		SyncLite instance = INSTANCES_BY_PRREFIXES.get(prefix);

		if (instance == null) {
			return null;
		}        
		return instance.createSyncLiteConnection(url, prop);
	}

	protected SQLiteConnection createSyncLiteConnection(String url, Properties prop) throws SQLException {
		throw new IllegalAccessError("Not implemented for base class SyncLite");
	}

	protected String getPrefix() {
		return PREFIX;
	}

	/**
	 * Gets the location to the database from a given URL.
	 * @param url The URL to extract the location from.
	 * @return The location to the database.
	 */
	static final String extractAddress(String url, String prefix) {
		int questionMarkIndex = url.indexOf('?');
		if (questionMarkIndex == -1) {
			return Path.of(url.substring(prefix.length())).toAbsolutePath().toString();
		}
		return Path.of(url.substring(prefix.length(), questionMarkIndex)).toAbsolutePath().toString();
	}

	public static void initialize(DeviceType deviceType, Path dbPath) throws SQLException {
		Logger tracer = null;
		try {
			SyncLiteOptions options = new SyncLiteOptions();
			tracer = initTracer(dbPath);
			INSTANCES_BY_DEVICE_TYPES.get(deviceType).initialize(dbPath.toAbsolutePath(), options, tracer);
		} catch (Exception e) {
			if(tracer != null) {
				tracer.error("Failed to initialize device at dbPath : " + dbPath + " : " + e.getMessage(), e);
			}
			throw e;
		}
	}

	public static final void initialize(DeviceType deviceType, Path dbPath, String deviceName) throws SQLException {
		Logger tracer = null;
		try {
			SyncLiteOptions options = new SyncLiteOptions();
			tracer = initTracer(dbPath.toAbsolutePath());
			options.setDeviceName(deviceName);
			INSTANCES_BY_DEVICE_TYPES.get(deviceType).initialize(dbPath.toAbsolutePath(), options, tracer);
		} catch (Exception e) {
			if(tracer != null) {
				tracer.error("Failed to initialize device at dbPath : " + dbPath + " : " + e.getMessage(), e);
			}
			throw e;
		}
	}

	public static final void initialize(DeviceType deviceType, Path dbPath, SyncLiteOptions options) throws SQLException {
		Logger tracer = null;
		try {
			//Make a deep copy of these options so that we don't end up mixing options for multiple devices.
			SyncLiteOptions copiedOptions = options.copy();
			if (options.getTracer() == null) {    	
				tracer = initTracer(dbPath);
				copiedOptions.setTracer(tracer);
			} else {
				tracer = copiedOptions.getTracer();
			}
			INSTANCES_BY_DEVICE_TYPES.get(deviceType).initialize(dbPath.toAbsolutePath(), copiedOptions, tracer);
		} catch(SQLException e) {
			if(tracer != null) {
				tracer.error("Failed to initialize device at dbPath : " + dbPath + " : " + e.getMessage(), e);
			}
			throw e;
		}
	}

	public static final void initialize(DeviceType deviceType, Path dbPath, SyncLiteOptions options, String deviceName) throws SQLException {
		Logger tracer = null;
		try {
			//Make a deep copy of these options so that we don't end up mixing options for multiple devices.
			SyncLiteOptions copiedOptions = options.copy();
			if (options.getTracer() == null) {    	
				tracer = initTracer(dbPath);
				copiedOptions.setTracer(tracer);
			} else {
				tracer = copiedOptions.getTracer();
			}
			options.setDeviceName(deviceName);
			INSTANCES_BY_DEVICE_TYPES.get(deviceType).initialize(dbPath.toAbsolutePath(), copiedOptions, tracer);
		} catch (SQLException e) {
			if(tracer != null) {
				tracer.error("Failed to initialize device at dbPath : " + dbPath + " : " + e.getMessage(), e);
			}
			throw e;
		}
	}

	public static final void initialize(DeviceType deviceType, Path dbPath, Path propsPath) throws SQLException {
		Logger tracer = null;
		try {
			tracer = initTracer(dbPath.toAbsolutePath());
			SyncLiteOptions options = SyncLiteOptions.loadAndValidateOptions(propsPath, tracer);
			INSTANCES_BY_DEVICE_TYPES.get(deviceType).initialize(dbPath.toAbsolutePath(), options, tracer);
		} catch (SQLException e) {
			if(tracer != null) {
				tracer.error("Failed to initialize device at dbPath : " + dbPath + " : " + e.getMessage(), e);
			}
			throw e;
		}
	}

	public static final void initialize(DeviceType deviceType, Path dbPath, Path propsPath, String deviceName) throws SQLException {
		Logger tracer = null;
		try {
			tracer = initTracer(dbPath);
			SyncLiteOptions options = SyncLiteOptions.loadAndValidateOptions(propsPath, tracer);
			options.setDeviceName(deviceName);
			INSTANCES_BY_DEVICE_TYPES.get(deviceType).initialize(dbPath.toAbsolutePath(), options, tracer);
		} catch (SQLException e) {
			if(tracer != null) {
				tracer.error("Failed to initialize device at dbPath : " + dbPath + " : " + e.getMessage(), e);
			}
			throw e;
		}
	}


	private final void initialize(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		Object lock = dbInitializationLocks.computeIfAbsent(dbPath, p -> new Object());

		//Synchronize access for a given dbPath.
		synchronized (lock) {
			if (SQLLogger.findInstance(dbPath) != null) {
				//throw new SQLException("SyncLite transactional duckdb device " + dbPath + " already initialized");
				return;
			}

			//Create SyncLite dir
			Path syncLiteDirPath = Path.of(dbPath.toString() + ".synclite");   	
			try {
				Files.createDirectories(syncLiteDirPath);
			} catch (IOException e) {
				throw new SQLException("Failed to create synclite directory : " + e.getMessage(), e);
			}

			if (requiresSQLiteSchemaFile()) {
				Path sqliteSchemaFilePath = getSQLiteSchemaFilePath(dbPath);
				prepareSQLiteSchemaFile(dbPath, sqliteSchemaFilePath, tracer, options);
			}

			Path defaultLocalStageDirectory = syncLiteDirPath;

			if (options.getNumDestinations() == 0) {
				options.setDestinationType(1, DestinationType.FS);
				options.setLocalDataStageDirectory(1, defaultLocalStageDirectory);
			} 
			for (Integer i = 1; i <= options.getNumDestinations(); ++i) {
				if (options.getLocalDataStageDirectory(i) == null) {
					options.setDestinationType(i, DestinationType.FS);
					options.setLocalDataStageDirectory(i, defaultLocalStageDirectory);
				} else {
					if (options.getDestinationType(i) == null) {
						options.setDestinationType(i, DestinationType.FS);
					}
				}
			}

			//Validate INTERNAL command handler if set  	
			if (options.getEnableCommandHandler()) {
				if (options.getCommandHandlerType() == CommandHandlerType.INTERNAL) {
					if (options.getCommandHanderCallback() == null) {
						throw new SQLException("No command handler callback registered by the application. It must be registered when INTERNAL command-handler is enabled.");
					}
				}
			}
			//Set device type
			setDeviceTypeInOptions(options);

			getOrCreateLoggerInstace(dbPath, options, tracer);

			addShutdownHook();
		}
	}

	protected boolean requiresSQLiteSchemaFile() {
		return true;
	}

	protected void getOrCreateLoggerInstace(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		throw new IllegalAccessError("Not implemented for base class SyncLite");
	}

	protected void setDeviceTypeInOptions(SyncLiteOptions options) throws SQLException {
		throw new IllegalAccessError("Not implemented for base class SyncLite");
	}

	protected void prepareSQLiteSchemaFile(Path dbPath, Path sqliteSchemaFilePath, Logger tracer, SyncLiteOptions options) throws SQLException {
		try {
			DBProcessor processor = getDBProcessor();
			processor.backupDB(dbPath, sqliteSchemaFilePath, options, true);	
		} catch (Exception e) {
			tracer.error("Failed to initialize sqlite schema file during initialization of specified db : " + dbPath + " : " + e.getMessage(), e);
			throw new SQLException("Failed to initialize sqlite schema file during initialization of specified db : " + dbPath + " : " + e.getMessage(), e);
		}
	}

	protected DBProcessor getDBProcessor() {
		throw new IllegalAccessError("Not implemented for base class SyncLite");
	}


	private static Logger initTracer(Path dbPath) {
		Path tracePath = Path.of(dbPath.toAbsolutePath().toString() + ".synclite", dbPath.getFileName().toString() + ".trace");
		Logger logger = Logger.getLogger(SQLLogger.class);
		logger.setLevel(Level.ERROR);
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("SyncLiteLogger");
		fa.setFile(tracePath.toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setMaxFileSize("10KB");
		fa.setAppend(true);
		fa.activateOptions();
		logger.addAppender(fa);
		return logger;
	}

	public static final void closeAllDevices() throws SQLException {    	
		SQLLogger.closeAllDevices();    	
	}

	public static final void closeDevice(Path dbPath) throws SQLException {
		//Delete metadata file so that it gets recreated on initialize of the device again
		Object lock = dbInitializationLocks.computeIfAbsent(dbPath, p -> new Object());
		synchronized (lock) {
			deleteSQLiteSchemaFileIfExists(dbPath);
			SQLLogger.closeDevice(dbPath.toAbsolutePath());
		}
	}

	private final static void deleteSQLiteSchemaFileIfExists(Path dbPath) {
		Path sqliteSchemaFilePath = getSQLiteSchemaFilePath(dbPath);
		try {
			if (Files.exists(sqliteSchemaFilePath)) {
				Files.delete(sqliteSchemaFilePath);
			}
		} catch (Exception e) {
			//Ignore
		}
	}

	public static final void closeAllDatabases() throws SQLException {    	
		closeAllDevices();
	}

	public static final void closeDatabase(Path dbPath) throws SQLException {
		closeDevice(dbPath);
	}

	public static final void reSynchronizeDevice(DeviceType deviceType, Path dbPath) throws SQLException {
		SyncLiteOptions options = SQLLogger.getSyncLiteOptions(dbPath);
		SQLLogger.cleanUpDevice(dbPath);
		//Bump up databaseID
		options.setDatabaseId(options.getDatabaseId() + 1);
		initialize(deviceType, dbPath, options);
	}

	public static final void reSynchronizeDatabase(DeviceType deviceType, Path dbPath) throws SQLException {
		reSynchronizeDevice(deviceType, dbPath);
	}

	protected final void addShutdownHook() {
		try {
			Runtime.getRuntime().addShutdownHook(new Thread()
			{
				public void run()
				{
					try {
						SQLLogger.closeAllDevices();
					} catch (SQLException e) {
						//tracer.error("SyncLite shutdown sequence had an exception : " + e.getMessage());
					}    			
					/*if (tracer != null) {
						tracer.getAppender("SyncLiteLogger").close();
					}*/
				}
			});
		} catch (IllegalStateException e) {
			//Ignore as this could be because the program is terminating ?
		}
	}


	static final String getLogSegmentSignature() {
		//return ".synclite.commandlog.";
		return ".sqllog";
	}

	static final String getDataFileSignature() {
		//return ".synclite.datafile.";
		return ".datafile";
	}

	static final String getSqlFileSignature() {
		//return ".synclite.datafile.";
		return ".sql";
	}

	static final String getTxnFileSignature() {
		//return ".synclite.datafile.";
		return ".txn";
	}

	static final Path getLogSegmentPath(Path dbPath, long databaseID, long seqNum) {
		//return Path.of(dbPath.toString() + ".synclite", dbPath.getFileName().toString() + getLogSegmentSignature() + databaseID + "."  + seqNum);
		return Path.of(dbPath.toString() + ".synclite", seqNum + getLogSegmentSignature());
	}

	static final Path getDataFilePath(Path dbPath, long databaseID, long seqNum) {
		//return Path.of(dbPath.toString() + ".synclite", dbPath.getFileName().toString() + getDataFileSignature() + databaseID + "." +seqNum);
		return Path.of(dbPath.toString() + ".synclite", seqNum + getDataFileSignature());
	}

	static final Path getTxnStageFilePath(Path dbPath, long txnID) {
		return Path.of(dbPath.toString() + ".synclite", txnID + getSqlFileSignature());
	}

	static final Path getTxnFilePath(Path dbPath, long logSeqNum, long txnID) {
		return Path.of(dbPath.toString() + ".synclite", logSeqNum + getLogSegmentSignature() + "." + txnID + getTxnFileSignature());
	}

	static final String getMetadataFileSuffix() {
		return ".synclite.metadata";
	}

	static final String getSQLiteSchemaFileSuffix() {
		return ".sqlite";
	}

	static final Path getSQLiteSchemaFilePath(Path dbPath) {
		return Path.of(dbPath.toString() + ".synclite",  dbPath.getFileName().toString() + getSQLiteSchemaFileSuffix());
	}

	static final Path getMetadataFilePath(Path dbPath) {
		return Path.of(dbPath.toString() + ".synclite",  dbPath.getFileName().toString() + getMetadataFileSuffix());
	}

	static final String getDataBackupSuffix() {
		return ".synclite.backup";
	}

	static final String getWriteArchiveNamePrefix() {
		return "synclite-";
	}

	static final String getReadArchiveNamePrefix() {
		return "synclite-";
	}

	public static boolean isTxnFileForLogSegment(long logSeqNum, Path p) {
		String prefix = logSeqNum + getLogSegmentSignature(); 
		return (p.getFileName().toString().startsWith(prefix) && p.getFileName().toString().endsWith(getTxnFileSignature()));
	}

}
