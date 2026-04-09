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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class DeviceWriter {
	private static final int MAX_SYNCLITE_DEVICE_NAME_LENGTH = 64;
	private static final String DEFAULT_DEVICE_NAME = "default";

	private static ConcurrentHashMap<Long, DeviceWriter> writers = new ConcurrentHashMap<Long, DeviceWriter>();
    private HashMap<String, PreparedStatement> topicStmts = new HashMap<String, PreparedStatement>();
    private HashMap<String, Long> topicBatchCounts = new HashMap<String, Long>();
    private HashMap<String, Long> topicBatchSizes = new HashMap<String, Long>();

    private String deviceName;
	private Path dbPath;
	private DeviceType deviceType; 
	private final Path deviceFilePath;
	private String deviceURL;
	private Connection deviceConn;
	private final LongSupplier operationIdSupplier;
	private final String topicColumnType;
	private final SyncLiteOptions options;
	private final long maxBatchSizeBytes;

	private boolean isInsideTxn;
	private DeviceWriter(Path dbPath, DeviceType deviceType, SyncLiteOptions options, long mBatchSizeBytes) throws SQLException {
		try {
	        this.deviceName = DEFAULT_DEVICE_NAME;
			this.options = options;
			this.options.setDeviceName(this.deviceName);
			this.dbPath = dbPath;
			this.deviceType = deviceType;
			this.deviceFilePath = this.dbPath.resolve(this.deviceName + ".db");
			this.deviceURL = "jdbc:synclite_streaming:" + deviceFilePath;
			switch (deviceType) {
			case STREAMING:
				this.deviceURL = "jdbc:synclite_streaming:" + deviceFilePath;
				Streaming.initialize(this.deviceFilePath, options);
				this.topicColumnType = "TEXT";
				break;
			case SQLITE_APPENDER:
				this.deviceURL = "jdbc:synclite_sqlite_appender:" + deviceFilePath;
				SQLiteAppender.initialize(this.deviceFilePath, options);
				this.topicColumnType = "TEXT";
				break;
			case DUCKDB_APPENDER:
				this.deviceURL = "jdbc:synclite_duckdb_appender:" + deviceFilePath;
				DuckDBAppender.initialize(this.deviceFilePath, options);
				this.topicColumnType = "TEXT";
				break;
			case DERBY_APPENDER:
				this.deviceURL = "jdbc:synclite_derby_appender:" + deviceFilePath;
				DerbyAppender.initialize(this.deviceFilePath, options);
				this.topicColumnType = "VARCHAR(32672)";
				break;
			case H2_APPENDER:
				this.deviceURL = "jdbc:synclite_h2_appender:" + deviceFilePath;
				H2Appender.initialize(this.deviceFilePath, options);
				this.topicColumnType = "VARCHAR(1048576)";
				break;
			case HYPERSQL_APPENDER:
				this.deviceURL = "jdbc:synclite_hsqldb_appender:" + deviceFilePath;
				HyperSQLAppender.initialize(this.deviceFilePath, options);
				this.topicColumnType = "LONGVARCHAR";
				break;
			default:
				throw new SQLException("Unsupported device type for KafkaProducer: " + deviceType
						+ ". Supported: STREAMING, SQLITE_APPENDER, DUCKDB_APPENDER, DERBY_APPENDER, H2_APPENDER, HYPERSQL_APPENDER");
			}			
			this.deviceConn = DriverManager.getConnection(deviceURL);
			if (this.deviceConn instanceof SyncLiteStoreConnection) {
				SyncLiteStoreConnection c = (SyncLiteStoreConnection) this.deviceConn;
				this.operationIdSupplier = c::getOperationId;
			} else {
				this.operationIdSupplier = () -> 0L;
			}
			this.deviceConn.setAutoCommit(false);
			this.isInsideTxn = false;
			this.maxBatchSizeBytes = mBatchSizeBytes;
		} catch (Exception e) {
			throw new SQLException("Failed to initialize device writer for device : " + deviceName, e);
		}
	}

	private void initTable(String topicName) throws SQLException {
		if (topicStmts.containsKey(topicName)) {
			return;
		}
		PreparedStatement pstmt;
		while (true) {
			try (Statement stmt = deviceConn.createStatement()) {
				try {
					stmt.execute("CREATE TABLE " + topicName
							+ " (key " + topicColumnType + ", value " + topicColumnType + ")");
				} catch (SQLException createEx) {
					// Not all backends support IF NOT EXISTS; ignore "table already exists" errors.
					String msg = createEx.getMessage() == null ? "" : createEx.getMessage().toLowerCase();
					String state = createEx.getSQLState() == null ? "" : createEx.getSQLState();
					if (!state.startsWith("X0Y32") && !msg.contains("already exists") && !msg.contains("already defined")) {
						throw createEx;
					}
				}
				pstmt = deviceConn.prepareStatement(
						"INSERT INTO " + topicName + " (key, value) VALUES (?, ?)");
				break;
			} catch (SQLException e) {
				if (e.getMessage() != null && e.getMessage().contains("SQLITE_BUSY")) {
					continue;
				}
				throw new SQLException("Failed to initialize topic : " + topicName + " : " + e.getMessage(), e);
			}
		}
		topicStmts.put(topicName, pstmt);
		topicBatchCounts.put(topicName, 0L);
		topicBatchSizes.put(topicName, 0L);
	}
	
	public static DeviceWriter getInstance(Path dbPath, DeviceType deviceType, SyncLiteOptions options, long maxBatchSizeBytes) {
		return writers.computeIfAbsent(Thread.currentThread().getId(), s -> {  
			DeviceWriter w;
			try {
				w = new DeviceWriter(dbPath, deviceType, options, maxBatchSizeBytes);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return w;
		});
	}

	public void begin() throws Exception {
		this.isInsideTxn = true;
	}

	public static void flush() throws SQLException {
		for (DeviceWriter dw : writers.values()) {
			dw.commit();
		}
	}
	
	public static void close() throws SQLException {		
		DeviceWriter lastWriter = null;
		for (DeviceWriter dw : writers.values()) {
			dw.commit();
			lastWriter = dw;
		}
		//Close device on one of the writers
		if (lastWriter != null) {
			lastWriter.closeDevice();
		}
	}
	
	private void closeDevice() throws SQLException {
		try {
			SyncLite.closeDevice(this.deviceFilePath);
		} catch (SQLException e) {
			throw new SQLException("Failed to close device : " + this.deviceFilePath + " : " + e.getMessage(), e);
		}
	}

	private void commit() throws SQLException {
		try {
			for (Map.Entry<String, PreparedStatement> pair : topicStmts.entrySet()) {
				String topicName = pair.getKey();
				PreparedStatement pstmt = pair.getValue();
				long topicBatchCount = topicBatchCounts.get(topicName);

				if (!pstmt.isClosed() && (topicBatchCount > 0)) {
					pstmt.executeBatch();
					pstmt.clearBatch();
					
					topicBatchCounts.put(topicName, 0L);
					topicBatchSizes.put(topicName, 0L);
				}
			}
			this.deviceConn.commit();
			this.isInsideTxn = false;
		} catch (SQLException e) {
			throw new SQLException("Failed to commit a transaction : " + e.getMessage(), e);
		}
	}

	private void rollback() throws SQLException {
		try {
			for (Map.Entry<String, PreparedStatement> pair : topicStmts.entrySet()) {
				String topicName = pair.getKey();
				PreparedStatement pstmt = pair.getValue();
				long topicBatchCount = topicBatchCounts.get(topicName);

				if (!pstmt.isClosed()) {
					pstmt.clearBatch();
					topicBatchCounts.put(topicName, 0L);
					topicBatchSizes.put(topicName, 0L);
				}
			}
			this.deviceConn.rollback();
			this.isInsideTxn = false;
		} catch (SQLException e) {
			throw new SQLException("Failed to rollback a transaction : " + e.getMessage(), e);
		}
	}

	public long write(String topicName, String key, String value) throws SQLException {
		try {
			initTable(topicName);
			PreparedStatement pstmt = topicStmts.get(topicName);
			pstmt.setString(1, key);
			pstmt.setString(2, value);
			pstmt.addBatch();
			
			long topicBatchCount = topicBatchCounts.get(topicName);
			++topicBatchCount;
			topicBatchCounts.put(topicName, topicBatchCount);
			long topicBatchSize =  topicBatchSizes.get(topicName);
			//TODO get byte sizes
			topicBatchSize+= (key.length() + value.length());
			topicBatchSizes.put(topicName, topicBatchSize);

			//Check batch size.
			if (!isInsideTxn) {
				boolean flushBatch = false;
				if (topicBatchSize >= maxBatchSizeBytes) {
					flushBatch = true;
				}
					
				if (flushBatch) {	
					//flush batch
					commit();
				}
			}
			return operationIdSupplier.getAsLong() - 2;
		} catch(SQLException e) {
			throw new SQLException("Failed to write a record : " + e.getMessage(), e);
		}
	}

}
