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

public class DeviceWriter {
	private static final int MAX_SYNCLITE_DEVICE_NAME_LENGTH = 64;
	private static final String DEFAULT_DEVICE_NAME = "default";
	private final String createTopicSqlTemplate = "CREATE TABLE IF NOT EXISTS $1 (key TEXT, value TEXT)";
	private final String insertTopicSqlTemplate = "INSERT INTO $1 (key,value) VALUES (?, ?)";

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
	private TelemetryConnection telemetryDeviceConn;
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
				break;
			case TELEMETRY:
				this.deviceURL = "jdbc:synclite_telemetry:" + deviceFilePath;
				Telemetry.initialize(this.deviceFilePath, options);
				break;
			case SQLITE_APPENDER:
				this.deviceURL = "jdbc:synclite_appender:" + deviceFilePath;
				SQLiteAppender.initialize(this.deviceFilePath, options);
				break;
			}			
			this.deviceConn = DriverManager.getConnection(deviceURL);
			this.telemetryDeviceConn = (TelemetryConnection) this.deviceConn;
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
				stmt.execute(createTopicSqlTemplate.replace("$1", topicName));
				pstmt = deviceConn.prepareStatement(insertTopicSqlTemplate.replace("$1", topicName));
				break;
			} catch (SQLException e) {
				if (e.getMessage().contains("SQLITE_BUSY")) {
					continue;
				} else {
					throw new SQLException("Failed to initialize topic : " + topicName + " : " + e.getMessage(), e);
				}
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
			Streaming.closeDevice(this.deviceFilePath);
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
			return telemetryDeviceConn.getOperationId() - 2;
		} catch(SQLException e) {
			throw new SQLException("Failed to write a record : " + e.getMessage(), e);
		}
	}

}
