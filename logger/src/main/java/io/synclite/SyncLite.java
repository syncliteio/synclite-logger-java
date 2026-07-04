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

package io.synclite;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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
		//Load embedded db drivers. SQLite is bundled (this class extends org.sqlite.JDBC).
		//The other drivers are optional: they are only needed if the application creates
		//a device of that type. We load them individually and swallow ClassNotFoundException
		//so a missing optional driver does not prevent the SyncLite runtime from starting.
		//If a device type whose driver is missing is later requested, DriverManager.getConnection
		//will surface a clear "No suitable driver" error at that point.
		loadOptionalDriver("org.sqlite.JDBC");
		
		boolean duckdbDriverAvailable = isDriverAvailable("org.duckdb.DuckDBDriver");
		if (duckdbDriverAvailable) {
			loadOptionalDriver("org.duckdb.DuckDBDriver");
		}
		loadOptionalDriver("org.apache.derby.jdbc.EmbeddedDriver");
		loadOptionalDriver("org.h2.Driver");
		loadOptionalDriver("org.hsqldb.jdbc.JDBCDriver");
		// PostgreSQL driver is needed by awaitSync to poll the destination's
		// synclite_checkpoint table when dstType=POSTGRES. JDBC 4 SPI
		// auto-discovery via META-INF/services is unreliable inside the
		// shaded fat jar (assembly merge can drop the service descriptor),
		// so register explicitly.
		loadOptionalDriver("org.postgresql.Driver");

		instance = new SQLite();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.SQLITE, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_sqlite:", instance);

		instance = new SQLiteAppender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.SQLITE_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_sqlite_appender:", instance);

		instance = new SQLiteStore();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.SQLITE_STORE, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_sqlite_store:", instance);

		if (duckdbDriverAvailable) {
			instance = new DuckDB();
			INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DUCKDB, instance);
			INSTANCES_BY_PRREFIXES.put("jdbc:synclite_duckdb:", instance);

			instance = new DuckDBAppender();
			INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DUCKDB_APPENDER, instance);
			INSTANCES_BY_PRREFIXES.put("jdbc:synclite_duckdb_appender:", instance);

			instance = new DuckDBStore();
			INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DUCKDB_STORE, instance);
			INSTANCES_BY_PRREFIXES.put("jdbc:synclite_duckdb_store:", instance);
		}

		instance = new Derby();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DERBY, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_derby:", instance);

		instance = new DerbyAppender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DERBY_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_derby_appender:", instance);

		instance = new DerbyStore();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DERBY_STORE, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_derby_store:", instance);

		instance = new H2();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.H2, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_h2:", instance);

		instance = new H2Appender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.H2_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_h2_appender:", instance);

		instance = new H2Store();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.H2_STORE, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_h2_store:", instance);

		instance = new HyperSQL();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.HYPERSQL, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_hsqldb:", instance);

		instance = new HyperSQLAppender();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.HYPERSQL_APPENDER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_hsqldb_appender:", instance);

		instance = new HyperSQLStore();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.HYPERSQL_STORE, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_hsqldb_store:", instance);

		instance = new DBLogger();
		INSTANCES_BY_DEVICE_TYPES.put(DeviceType.DBLOGGER, instance);
		INSTANCES_BY_PRREFIXES.put("jdbc:synclite_dblogger:", instance);

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

	private static void loadOptionalDriver(String driverClassName) {
		try {
			Class.forName(driverClassName);
		} catch (ClassNotFoundException e) {
			//Optional driver not on classpath. Only required if the application
			//uses the corresponding device type; surfaced later by DriverManager.
		}
	}

	static boolean isDriverAvailable(String driverClassName) {
		try {
			Class.forName(driverClassName);
			return true;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

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
		validateLibs(tracer);

		Object lock = dbInitializationLocks.computeIfAbsent(dbPath, p -> new Object());

		//Synchronize access for a given dbPath.
		synchronized (lock) {
			setDeviceTypeInOptions(options);

			SQLLogger existingLogger = SQLLogger.findInstance(dbPath);
			if (existingLogger != null) {
				String configuredDeviceName = options.getDeviceName();
				if (configuredDeviceName != null && !configuredDeviceName.isEmpty() && existingLogger.getDeviceName() != null
						&& !configuredDeviceName.equals(existingLogger.getDeviceName())) {
					throw new SQLException("SyncLite : This device metadata name : " + existingLogger.getDeviceName()
							+ " does not match configured device name : " + configuredDeviceName);
				}
				DeviceType configuredDeviceType = options.getDeviceType();
				if (configuredDeviceType != null && existingLogger.getDeviceType() != null
						&& !configuredDeviceType.toString().equalsIgnoreCase(existingLogger.getDeviceType().toString())) {
					throw new SQLException("SyncLite : This device metadata type : " + existingLogger.getDeviceType()
							+ " does not match configured device type : " + configuredDeviceType);
				}
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

	protected void validateLibs(Logger tracer) throws SQLException {
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
		Logger logger = Logger.getLogger(SQLLogger.class.getName() + "." + tracePath.toAbsolutePath().normalize().toString());
		logger.setLevel(Level.ERROR);
		logger.setAdditivity(false);
		if (logger.getAppender("SyncLiteLogger") != null) {
			return logger;
		}
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
		Path absDb = dbPath.toAbsolutePath();

		SQLException loggerErr = null;

		Object lock = dbInitializationLocks.computeIfAbsent(dbPath, p -> new Object());
		synchronized (lock) {
			try {
				deleteSQLiteSchemaFileIfExists(dbPath);
				SQLLogger.closeDevice(absDb);
			} catch (SQLException e) {
				loggerErr = e;
			}
		}

		// Tear down the in-process consolidator if this device was
		// initialized with a DestinationOptions overload.
		DeviceState state = DEVICES.remove(absDb);
		if (state != null) {
			try {
				NativeConsolidator.nativeStopConsolidator(state.handle);
			} catch (RuntimeException e) {
				if (loggerErr != null) {
					loggerErr.addSuppressed(e);
				} else {
					throw new SQLException(
							"failed to stop native consolidator: " + e.getMessage(), e);
				}
			}
		}

		if (loggerErr != null) {
			throw loggerErr;
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

	static final Path getLogSegmentPath(Path dbPath, long seqNum) {
		return Path.of(dbPath.toString() + ".synclite", seqNum + getLogSegmentSignature());
	}

	static final Path getDataFilePath(Path dbPath, long seqNum) {
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

	// =========================================================================
	// In-process consolidator runtime (lazy JNI; only touched when a caller
	// uses the DestinationOptions-flavored initialize overloads).
	// =========================================================================

	private static final ConcurrentHashMap<Path, DeviceState> DEVICES = new ConcurrentHashMap<>();
	private static final AtomicBoolean SHUTDOWN_HOOK_INSTALLED = new AtomicBoolean(false);

	public static void initialize(
			DeviceType deviceType,
			Path dbPath,
			DestinationOptions destination) throws SQLException {
		SyncLiteOptions opts = new SyncLiteOptions();
		applyRuntimeStageDefaults(opts, dbPath);
		doInitializeWithDestination(deviceType, dbPath, opts, destination, null);
	}

	public static void initialize(
			DeviceType deviceType,
			Path dbPath,
			String deviceName,
			DestinationOptions destination) throws SQLException {
		SyncLiteOptions opts = new SyncLiteOptions();
		opts.setDeviceName(deviceName);
		applyRuntimeStageDefaults(opts, dbPath);
		doInitializeWithDestination(deviceType, dbPath, opts, destination, null);
	}

	public static void initialize(
			DeviceType deviceType,
			Path dbPath,
			SyncLiteOptions options,
			DestinationOptions destination) throws SQLException {
		applyRuntimeStageDefaults(options, dbPath);
		doInitializeWithDestination(deviceType, dbPath, options, destination, null);
	}

	public static void initialize(
			DeviceType deviceType,
			Path dbPath,
			SyncLiteOptions options,
			String deviceName,
			DestinationOptions destination) throws SQLException {
		options.setDeviceName(deviceName);
		applyRuntimeStageDefaults(options, dbPath);
		doInitializeWithDestination(deviceType, dbPath, options, destination, null);
	}

	public static void initialize(
			DeviceType deviceType,
			Path dbPath,
			Path propsPath,
			DestinationOptions destination) throws SQLException {
		SyncLiteOptions opts = SyncLiteOptions.loadFromFile(propsPath);
		Path workDir = workDirFromProperties(propsPath);
		applyRuntimeStageDefaults(opts, dbPath);
		doInitializeWithDestination(deviceType, dbPath, opts, destination, workDir);
	}

	public static void initialize(
			DeviceType deviceType,
			Path dbPath,
			Path propsPath,
			String deviceName,
			DestinationOptions destination) throws SQLException {
		SyncLiteOptions opts = SyncLiteOptions.loadFromFile(propsPath);
		Path workDir = workDirFromProperties(propsPath);
		opts.setDeviceName(deviceName);
		applyRuntimeStageDefaults(opts, dbPath);
		doInitializeWithDestination(deviceType, dbPath, opts, destination, workDir);
	}

	/**
	 * Pause destination consolidation for the device at {@code dbPath}.
	 * The Java logger keeps appending and rolling local segments; only
	 * the consolidator's apply-to-destination step pauses.
	 */
	public static void pauseSync(Path dbPath) throws SQLException {
		try {
			NativeConsolidator.nativePauseSync(dbPath.toAbsolutePath().toString());
		} catch (RuntimeException e) {
			throw new SQLException("pauseSync failed: " + e.getMessage(), e);
		}
	}

	/** Resume destination consolidation for the device at {@code dbPath}. */
	public static void resumeSync(Path dbPath) throws SQLException {
		try {
			NativeConsolidator.nativeResumeSync(dbPath.toAbsolutePath().toString());
		} catch (RuntimeException e) {
			throw new SQLException("resumeSync failed: " + e.getMessage(), e);
		}
	}

	/** {@code true} when {@link #pauseSync(Path)} is in effect for {@code dbPath}. */
	public static boolean isSyncPaused(Path dbPath) throws SQLException {
		try {
			return NativeConsolidator.nativeIsSyncPaused(dbPath.toAbsolutePath().toString());
		} catch (RuntimeException e) {
			throw new SQLException("isSyncPaused failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Wipe per-device local state and (when reachable) delete this
	 * device's metadata rows from the destination so the next
	 * {@code initialize(..)} re-seeds the device from scratch as the
	 * <em>same logical device</em>: the UUID, device-name, device-type
	 * and destination wiring are preserved, but the segment sequence
	 * restarts at 0 and a fresh initial backup is taken.
	 *
	 * <p>The user's source DB file is left untouched. A sentinel file
	 * dropped under the device home causes the next
	 * {@code initialize(..)} call to force
	 * {@code dst-object-init-mode-1=OVERWRITE_OBJECT} for the
	 * post-reinit re-seed only: in {@code dst-sync-mode=REPLICATION}
	 * the destination tables are dropped and recreated; in
	 * {@code dst-sync-mode=CONSOLIDATION} this device's rows are
	 * truncated on the shared destination. Either way the re-seed
	 * lands cleanly with no duplicates, and no user-visible
	 * configuration is mutated.
	 *
	 * <p>Idempotent across invocations: if the destination metadata
	 * cleanup transaction fails the local state is left intact and
	 * the call can be retried.
	 *
	 * @param dbPath full path to the device DB file
	 */
	public static void reinitialize(Path dbPath) throws SQLException {
		try {
			NativeConsolidator.nativeReinitialize(
					dbPath.toAbsolutePath().toString());
		} catch (RuntimeException e) {
			throw new SQLException("reinitialize failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Block until the in-process consolidator has applied every commit
	 * the device has produced, or {@code timeout} elapses.
	 *
	 * <p>Source-of-truth contract (applies to every destination type):
	 * <ul>
	 *   <li><b>Source side</b> &mdash; latest commit id is always
	 *       {@code MAX(commit_id)} from the user DB file's
	 *       {@code synclite_txn} table. See
	 *       {@link #readSourceCommitIdForAwaitSync(Path)}.</li>
	 *   <li><b>Applied side</b> &mdash; latest applied commit id is
	 *       read from the destination's {@code synclite_checkpoint}
	 *       table (the consolidator updates it co-transactionally with
	 *       each apply batch). We open a fresh JDBC connection to the
	 *       destination here, qualified by the configured schema if any,
	 *       and poll {@code MAX(commit_id)} until it catches up. This
	 *       is the only place that gives a crash-safe, restart-safe
	 *       answer &mdash; the consolidator's local-mirror checkpoint
	 *       cannot.</li>
	 * </ul>
	 *
	 * <p>If a device was initialized via the logger-only overload (no
	 * {@link DestinationOptions}) we have no destination to poll and
	 * fall back to {@code nativeAwaitAppliedCommit}, which is good
	 * enough because in that mode there IS no in-process apply.
	 */
	public static void awaitSync(Path dbPath, Duration timeout) throws SQLException {
		Path absDb = dbPath.toAbsolutePath();
		long targetCommitId = readSourceCommitIdForAwaitSync(absDb);
		if (targetCommitId <= 0L) {
			// No source-side commits => nothing to wait for. Legitimate
			// fast-path (e.g. device opened but no writes), not an error.
			return;
		}
		long timeoutMs = (timeout == null || timeout.isNegative())
				? 0L : timeout.toMillis();

		DeviceState state = DEVICES.get(absDb);
		if (state != null && state.dstType != null) {
			awaitSyncOnDestination(absDb, state, targetCommitId, timeoutMs);
			return;
		}
		// Logger-only mode: no in-process consolidator was spawned, and
		// no destination to poll. Hand off to native (which itself will
		// just confirm there's no pending stage and return).
		try {
			NativeConsolidator.nativeAwaitAppliedCommit(absDb.toString(), targetCommitId, timeoutMs);
		} catch (RuntimeException e) {
			throw new SQLException("awaitSync failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Poll the destination's {@code synclite_checkpoint} table until
	 * {@code commit_id >= targetCommitId} for this device, or
	 * {@code timeoutMs} elapses.
	 *
	 * <p>Behavior across destination types:
	 * <ul>
	 *   <li>{@link DstType#POSTGRES POSTGRES}: connects via the bundled
	 *       PG JDBC driver, qualifies the checkpoint table with the
	 *       configured schema.</li>
	 *   <li>{@link DstType#DUCKDB DUCKDB}: opens the destination file
	 *       read-only via {@code jdbc:duckdb:...}, qualifies with the
	 *       configured schema if any.</li>
	 *   <li>{@link DstType#SQLITE SQLITE}: opens the destination file
	 *       read-only via {@code jdbc:sqlite:...?open_mode=1}, no
	 *       schema concept.</li>
	 * </ul>
	 *
	 * <p>A missing checkpoint table (or zero rows) is treated as
	 * "0 applied"; the loop keeps waiting because the consolidator
	 * creates and seeds the table on its first successful apply. The
	 * timeout error includes whether the table existed plus the last
	 * underlying SQL error, so you can tell "consolidator never started"
	 * from "consolidator is behind".
	 */
	private static void awaitSyncOnDestination(Path absDb, DeviceState state,
			long targetCommitId, long timeoutMs) throws SQLException {
		String jdbcUrl = buildDestinationJdbcUrl(state);
		if (jdbcUrl == null) {
			throw new SQLException("awaitSync: cannot derive JDBC URL for destination type "
					+ state.dstType + " (connection_string=" + state.dstConnectionString + ")");
		}
		String checkpointTable = qualifiedCheckpointTable(state);
		String selectSql = "SELECT MAX(commit_id) FROM " + checkpointTable
				+ " WHERE synclite_device_id = ? AND synclite_device_name = ?";

		long deadlineNanos = timeoutMs <= 0L
				? Long.MAX_VALUE
				: System.nanoTime() + Duration.ofMillis(timeoutMs).toNanos();
		long appliedCommitId = 0L;
		boolean checkpointTableMissing = true;
		SQLException lastError = null;
		while (true) {
			try (Connection conn = DriverManager.getConnection(jdbcUrl);
					PreparedStatement ps = conn.prepareStatement(selectSql)) {
				ps.setString(1, state.deviceId == null ? "" : state.deviceId);
				ps.setString(2, state.deviceName == null ? "" : state.deviceName);
				try (ResultSet rs = ps.executeQuery()) {
					checkpointTableMissing = false;
					if (rs.next()) {
						appliedCommitId = rs.getLong(1);
						if (rs.wasNull()) {
							appliedCommitId = 0L;
						}
					}
				}
				lastError = null;
			} catch (SQLException e) {
				// Most common transient: checkpoint relation does not exist
				// yet (PG SQLSTATE 42P01, SQLite/DuckDB "no such table") —
				// the consolidator creates it on first successful apply.
				// Other errors (connectivity, auth, file lock) propagate
				// after timeout.
				lastError = e;
			}
			if (appliedCommitId >= targetCommitId) {
				return;
			}
			if (System.nanoTime() >= deadlineNanos) {
				StringBuilder msg = new StringBuilder("awaitSync: timed out after ")
						.append(timeoutMs).append("ms waiting for destination ")
						.append(state.dstType)
						.append(" (target_commit_id=").append(targetCommitId)
						.append(", applied_commit_id=").append(appliedCommitId)
						.append(", db=").append(absDb)
						.append(", checkpoint_table=").append(checkpointTable)
						.append(")");
				if (checkpointTableMissing) {
					msg.append(" \u2014 ").append(checkpointTable)
							.append(" does not exist yet; consolidator has not completed a successful apply.");
				}
				if (lastError != null) {
					msg.append(" \u2014 last error: ").append(lastError.getMessage());
					throw new SQLException(msg.toString(), lastError);
				}
				throw new SQLException(msg.toString());
			}
			try {
				Thread.sleep(200L);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				throw new SQLException("awaitSync: interrupted while waiting", ie);
			}
		}
	}

	/**
	 * Build a JDBC URL pointing at the destination for read-only
	 * polling. Accepts the user's raw {@code DestinationOptions#connectionString}
	 * in whichever form they supplied (JDBC URL, libpq URL, or bare file path).
	 */
	private static String buildDestinationJdbcUrl(DeviceState state) {
		String raw = state.dstConnectionString;
		if (raw == null) {
			return null;
		}
		String trimmed = raw.trim();
		if (trimmed.isEmpty()) {
			return null;
		}
		switch (state.dstType) {
			case POSTGRES:
				if (trimmed.regionMatches(true, 0, "jdbc:", 0, 5)) {
					return trimmed;
				}
				if (trimmed.regionMatches(true, 0, "postgresql://", 0, 13)
						|| trimmed.regionMatches(true, 0, "postgres://", 0, 11)) {
					return "jdbc:" + trimmed;
				}
				return trimmed;
			case DUCKDB:
				if (trimmed.regionMatches(true, 0, "jdbc:duckdb:", 0, 12)) {
					return trimmed;
				}
				return "jdbc:duckdb:" + trimmed;
			case SQLITE:
				// Read-only open avoids any lock contention with the
				// consolidator's writer connection.
				if (trimmed.regionMatches(true, 0, "jdbc:sqlite:", 0, 12)) {
					return trimmed.contains("?")
							? trimmed
							: trimmed + "?open_mode=1";
				}
				return "jdbc:sqlite:" + trimmed.replace('\\', '/') + "?open_mode=1";
			default:
				return null;
		}
	}

	/**
	 * Return the destination's {@code synclite_checkpoint} table name,
	 * qualified by schema where supported. Identifier quoting matches
	 * the destination engine's conventions (double-quote for PG/DuckDB
	 * standard SQL; SQLite is unqualified single-DB so no quoting needed).
	 */
	private static String qualifiedCheckpointTable(DeviceState state) {
		String schema = state.dstSchema == null ? null : state.dstSchema.trim();
		switch (state.dstType) {
			case POSTGRES:
				if (schema == null || schema.isEmpty()) {
					return "synclite_checkpoint";
				}
				return quoteSqlIdent(schema) + "." + quoteSqlIdent("synclite_checkpoint");
			case DUCKDB:
				if (schema == null || schema.isEmpty()) {
					return "synclite_checkpoint";
				}
				return quoteSqlIdent(schema) + "." + quoteSqlIdent("synclite_checkpoint");
			case SQLITE:
			default:
				return "synclite_checkpoint";
		}
	}

	/** Quote a SQL identifier with double quotes (standard SQL; works on PG and DuckDB). */
	private static String quoteSqlIdent(String ident) {
		return "\"" + ident.replace("\"", "\"\"") + "\"";
	}

	/**
	 * Resolve the latest source-side commit id for {@code absDb}.
	 *
	 * <p>Source of truth is the {@code synclite_txn} table that lives
	 * inside the user DB file. Every user commit advances
	 * {@code commit_id} in that table atomically with the user-data
	 * write, so {@code MAX(commit_id)} from a fresh, plain JDBC
	 * connection (not the {@code synclite_*} wrapper) is the durable,
	 * crash-safe answer — and it survives the user closing their
	 * SyncLite connection, which the in-memory commit-id tracker does
	 * not.
	 *
	 * <p>Routing:
	 * <ul>
	 *   <li>SQLite-native devices ({@code SQLITE}, {@code SQLITE_APPENDER},
	 *       {@code SQLITE_STORE}, {@code STREAMING}, {@code DBLOGGER}):
	 *       open {@code jdbc:sqlite:&lt;path&gt;} read-only.</li>
	 *   <li>DuckDB-native devices: open {@code jdbc:duckdb:&lt;path&gt;}.</li>
	 *   <li>JDBC-bridge devices (Derby / H2 / HyperSQL): delegate to
	 *       {@link MultiWriterDBProcessor#readMaxSourceCommitId(Path)},
	 *       which already knows how to reach the backend.</li>
	 * </ul>
	 *
	 * <p>Throws on failure rather than silently returning 0; a "0"
	 * answer would short-circuit {@code await_applied_commit} into a
	 * false-positive "succeeded" without ever proving the consolidator
	 * applied anything.
	 */
	private static long readSourceCommitIdForAwaitSync(Path absDb) throws SQLException {
		DeviceState state = DEVICES.get(absDb);
		String jdbcUrl = buildPlainJdbcUrlForDevice(absDb, state);
		if (jdbcUrl != null) {
			try (Connection conn = DriverManager.getConnection(jdbcUrl);
					Statement stmt = conn.createStatement();
					ResultSet rs = stmt.executeQuery("SELECT MAX(commit_id) FROM synclite_txn")) {
				if (rs.next()) {
					long v = rs.getLong(1);
					return v < 0 ? 0L : v;
				}
				return 0L;
			} catch (SQLException e) {
				throw new SQLException(
						"awaitSync: failed to read source commit id from "
								+ absDb + " : " + e.getMessage(), e);
			}
		}
		// JDBC-bridge devices: the user "DB file" is actually a JDBC
		// backend (Derby / H2 / HyperSQL); route through the device's
		// MultiWriterDBProcessor.
		if (state != null && state.deviceType != null) {
			SyncLite syncLite = INSTANCES_BY_DEVICE_TYPES.get(state.deviceType);
			if (syncLite != null) {
				DBProcessor proc = syncLite.getDBProcessor();
				if (proc instanceof MultiWriterDBProcessor) {
					return ((MultiWriterDBProcessor) proc).readMaxSourceCommitId(absDb);
				}
			}
		}
		throw new SQLException(
				"awaitSync: could not resolve source commit id for "
						+ absDb + " (device not initialized via SyncLite.initialize)");
	}

	/**
	 * Build a plain JDBC URL for opening the user DB file directly
	 * (bypassing the {@code synclite_*} wrapper) to read
	 * {@code synclite_txn}. Returns {@code null} for JDBC-bridge device
	 * types whose backing store is not a single local file.
	 *
	 * <p>SQLite URLs request read-only mode via {@code open_mode=1}
	 * (SQLITE_OPEN_READONLY) so that opening this connection while the
	 * user's writer connection is still alive cannot acquire any
	 * conflicting locks.
	 */
	private static String buildPlainJdbcUrlForDevice(Path absDb, DeviceState state) {
		DeviceType t = (state != null) ? state.deviceType : null;
		// Fall back to extension sniffing only when the device has not
		// been registered yet (very early call from a test harness).
		if (t == null) {
			String n = absDb.getFileName().toString().toLowerCase();
			if (n.endsWith(".duckdb")) {
				return "jdbc:duckdb:" + absDb;
			}
			return "jdbc:sqlite:" + absDb.toString().replace('\\', '/')
					+ "?open_mode=1";
		}
		switch (t) {
			case SQLITE:
			case SQLITE_APPENDER:
			case SQLITE_STORE:
			case STREAMING:
			case DBLOGGER:
				return "jdbc:sqlite:" + absDb.toString().replace('\\', '/')
						+ "?open_mode=1";
			case DUCKDB:
			case DUCKDB_APPENDER:
			case DUCKDB_STORE:
				return "jdbc:duckdb:" + absDb;
			default:
				return null;
		}
	}

	/** Snapshot of the device's consolidator run state + latest heartbeat row. */
	public static SyncStatus syncStatus(Path dbPath) throws SQLException {
		Object[] raw;
		try {
			raw = NativeConsolidator.nativeSyncStatus(dbPath.toAbsolutePath().toString());
		} catch (RuntimeException e) {
			throw new SQLException("syncStatus failed: " + e.getMessage(), e);
		}
		int ord = ((Integer) raw[0]).intValue();
		SyncState[] all = SyncState.values();
		SyncState state = (ord >= 0 && ord < all.length) ? all[ord] : SyncState.NOT_INITIALIZED;
		return new SyncStatus(state, (String) raw[1], (String) raw[2],
				((Long) raw[3]).longValue());
	}

	/** Snapshot of consolidator counters for {@code dbPath}. */
	public static SyncStatistics syncStatistics(Path dbPath) throws SQLException {
		long[] v;
		try {
			v = NativeConsolidator.nativeSyncStatistics(dbPath.toAbsolutePath().toString());
		} catch (RuntimeException e) {
			throw new SQLException("syncStatistics failed: " + e.getMessage(), e);
		}
		return new SyncStatistics(v[0], v[1], v[2], v[3], v[4], v[5]);
	}

	/** Snapshot of wall-clock sync lag between the device and the destination. */
	public static SyncLatency syncLatency(Path dbPath) throws SQLException {
		long[] v;
		try {
			v = NativeConsolidator.nativeSyncLatency(dbPath.toAbsolutePath().toString());
		} catch (RuntimeException e) {
			throw new SQLException("syncLatency failed: " + e.getMessage(), e);
		}
		OptionalLong applied = (v[1] == Long.MIN_VALUE)
				? OptionalLong.empty() : OptionalLong.of(v[1]);
		return new SyncLatency(v[0], applied, v[2]);
	}

	// ---------- core ----------------------------------------------------

	private static void doInitializeWithDestination(
			DeviceType deviceType,
			Path dbPath,
			SyncLiteOptions options,
			DestinationOptions destination,
			Path workDirOverride) throws SQLException {

		Objects.requireNonNull(deviceType, "deviceType");
		Objects.requireNonNull(dbPath, "dbPath");
		Objects.requireNonNull(destination, "destination");

		Path absDb = dbPath.toAbsolutePath();
		if (DEVICES.containsKey(absDb)) {
			return; // idempotent
		}

		Path stageDir = options.getLocalDataStageDirectory(1);
		Path workDir  = (workDirOverride != null) ? workDirOverride : defaultWorkDir();
		try {
			Files.createDirectories(stageDir);
			Files.createDirectories(workDir);
		} catch (IOException e) {
			throw new SQLException("failed to create stage/work directories", e);
		}

		// 1. Bring up the Java logger.
		initialize(deviceType, absDb, options);

		// 2. Resolve uuid + device name the logger persisted.
		String deviceId = readDeviceUuidFromMetadata(absDb);
		String deviceName = options.getDeviceName();
		if (deviceName == null || deviceName.isEmpty()) {
			deviceName = readDeviceNameFromMetadata(absDb, "");
		}
		Path perDeviceStage = (deviceName == null || deviceName.isEmpty())
				? stageDir.resolve("synclite-" + deviceId)
				: stageDir.resolve("synclite-" + deviceName + "-" + deviceId);

		// Periodic stage-scan tick. Mirrors the Java consolidator's
		// `device-polling-interval-ms` knob: the in-process Rust
		// consolidator scans `perDeviceStage` for unapplied segments
		// every tick, providing the safety floor below the push-style
		// `nativeNotifyStagePath` notifications that some Java
		// shipping paths skip on Windows.
		long devicePollingIntervalMs = Long.getLong(
				"synclite.device.polling.interval.ms", 500L);

		// 3. Spawn the Rust consolidator pointed at the same stage dir.
		long handle;
		try {
			handle = NativeConsolidator.nativeSpawnConsolidator(
					workDir.toString(),
					workDir.toString(),
					deviceId,
					deviceName == null ? "" : deviceName,
					deviceType.name(),
					databaseNameOf(absDb),
					destination.dstType().name(),
					destination.connectionString(),
					destination.syncMode().name(),
					destination.database().orElse(null),
					destination.schema().orElse(null),
					"DESTINATION",
					perDeviceStage.toString(),
					devicePollingIntervalMs);
		} catch (RuntimeException e) {
			try { closeDevice(absDb); } catch (Exception ignore) {}
			throw new SQLException(
					"failed to spawn native consolidator: " + e.getMessage(), e);
		}

		// 4. Catch up on segments left behind by a previous run.
		try {
			if (Files.isDirectory(perDeviceStage)) {
				Path backup = perDeviceStage.resolve(absDb.getFileName().toString() + ".synclite.backup");
				Path metadata = perDeviceStage.resolve(absDb.getFileName().toString() + ".synclite.metadata");
				if (Files.exists(backup) && Files.exists(metadata)) {
					NativeConsolidator.nativeNotifyBootstrapReady(
							handle, backup.toString(), metadata.toString());
				}
				NativeConsolidator.nativeCatchUpStageDir(handle, perDeviceStage.toString());
			}
		} catch (RuntimeException e) {
			safeStopAndClose(handle, absDb);
			throw new SQLException(
					"failed to catch up stage directory " + perDeviceStage + ": " + e.getMessage(), e);
		}

		// 5. Pre-create the per-device stage subdir. The Rust
		// consolidator scans this directory periodically (controlled
		// by `synclite.device.polling.interval.ms`) and applies any
		// segment the upstream shipper drops in. No host-side
		// WatchService is needed.
		try {
			Files.createDirectories(perDeviceStage);
		} catch (IOException e) {
			safeStopAndClose(handle, absDb);
			throw new SQLException(
					"failed to create per-device stage " + perDeviceStage + ": " + e.getMessage(), e);
		}

		DEVICES.put(absDb, new DeviceState(
				handle,
				deviceType,
				destination.dstType(),
				destination.connectionString(),
				destination.schema().orElse(null),
				deviceId,
				deviceName == null ? "" : deviceName));
		installShutdownHook();
	}

	// ---------- helpers --------------------------------------------------

	/**
	 * Make sure {@code options} has destination 1 pointed at a usable
	 * local stage directory; honors caller-set values and falls back to
	 * {@code ~/synclite/job1/stageDir}. Pins destination 1 to FS since
	 * the in-process consolidator only consumes file-system segments.
	 */
	private static void applyRuntimeStageDefaults(SyncLiteOptions options, Path dbPath) {
		Path stageDir = options.getLocalDataStageDirectory(1);
		if (stageDir == null) {
			stageDir = defaultStageDir();
			options.setLocalDataStageDirectory(1, stageDir);
		}
		if (options.getDestinationType(1) == null) {
			options.setDestinationType(1, DestinationType.FS);
		}
	}

	private static Path workDirFromProperties(Path propsPath) throws SQLException {
		Properties props = new Properties();
		try (java.io.InputStream in = Files.newInputStream(propsPath)) {
			props.load(in);
		} catch (IOException e) {
			throw new SQLException("failed to read conf " + propsPath + ": " + e.getMessage(), e);
		}
		String v = props.getProperty("device-data-root");
		if (v == null) {
			v = props.getProperty("device-data-root-1");
		}
		if (v == null || v.trim().isEmpty()) {
			return null;
		}
		return Paths.get(v.trim());
	}

	private static Path defaultStageDir() {
		return userHome().resolve("synclite").resolve("job1").resolve("stageDir");
	}

	private static Path defaultWorkDir() {
		return userHome().resolve("synclite").resolve("job1").resolve("workDir");
	}

	private static Path userHome() {
		String home = System.getProperty("user.home");
		if (home == null || home.isEmpty()) {
			return Paths.get(".").toAbsolutePath().normalize();
		}
		return Paths.get(home);
	}

	private static String databaseNameOf(Path absDb) {
		String name = absDb.getFileName().toString();
		int dot = name.lastIndexOf('.');
		return dot > 0 ? name.substring(0, dot) : name;
	}

	private static String readDeviceUuidFromMetadata(Path absDb) throws SQLException {
		return readMetadataKey(absDb, "uuid", null,
				"uuid not found in logger metadata file");
	}

	private static String readDeviceNameFromMetadata(Path absDb, String fallback) throws SQLException {
		return readMetadataKey(absDb, "device_name", fallback, null);
	}

	private static String readMetadataKey(
			Path absDb, String key, String fallback, String missingMsg) throws SQLException {
		Path metadataFile = Paths.get(
				absDb.toString() + ".synclite",
				absDb.getFileName().toString() + ".synclite.metadata");
		if (!Files.exists(metadataFile)) {
			if (missingMsg != null) {
				throw new SQLException("logger metadata file missing: " + metadataFile);
			}
			return fallback;
		}
		String url = "jdbc:sqlite:" + metadataFile.toString().replace('\\', '/');
		try (Connection c = DriverManager.getConnection(url);
		     Statement s = c.createStatement();
		     ResultSet rs = s.executeQuery(
		             "SELECT value FROM metadata WHERE key = '" + key + "' LIMIT 1")) {
			if (rs.next()) {
				String v = rs.getString(1);
				if (v != null && !v.isEmpty()) {
					return v;
				}
			}
			if (missingMsg != null) {
				throw new SQLException(missingMsg + ": " + metadataFile);
			}
			return fallback;
		}
	}

	private static void safeStopAndClose(long handle, Path absDb) {
		try { NativeConsolidator.nativeStopConsolidator(handle); } catch (Exception ignore) {}
		try { closeDevice(absDb); } catch (Exception ignore) {}
	}

	private static void installShutdownHook() {
		if (!SHUTDOWN_HOOK_INSTALLED.compareAndSet(false, true)) {
			return;
		}
		Runtime.getRuntime().addShutdownHook(new Thread(SyncLite::shutdownAll,
				"synclite-consolidator-shutdown"));
	}

	private static void shutdownAll() {
		for (Path db : DEVICES.keySet().toArray(new Path[0])) {
			try {
				closeDevice(db);
			} catch (Throwable ignore) {
				// best effort during JVM shutdown
			}
		}
	}

	static final class DeviceState {
		final long handle;
		final DeviceType deviceType;
		/** Destination snapshot — captured at initialize() so awaitSync can
		 *  query the true source-of-truth (destination synclite_checkpoint)
		 *  for POSTGRES targets instead of trusting the consolidator's
		 *  local-mirror checkpoint. May be null for logger-only mode. */
		final DstType dstType;
		final String dstConnectionString;
		final String dstSchema;
		final String deviceId;
		final String deviceName;
		DeviceState(long handle, DeviceType deviceType,
				DstType dstType, String dstConnectionString,
				String dstSchema, String deviceId, String deviceName) {
			this.handle = handle;
			this.deviceType = deviceType;
			this.dstType = dstType;
			this.dstConnectionString = dstConnectionString;
			this.dstSchema = dstSchema;
			this.deviceId = deviceId;
			this.deviceName = deviceName;
		}
	}

}
