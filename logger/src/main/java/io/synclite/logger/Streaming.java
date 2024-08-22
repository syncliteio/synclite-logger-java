package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.sqlite.SQLiteConnection;

public final class Streaming extends SyncLite {

	private final static String PREFIX = "jdbc:synclite_streaming:";
	
    @Override
	protected final SQLiteConnection createSyncLiteConnection(String url, Properties prop) throws SQLException {
        if (!checkDeviceURL(url)) {
            return null;       
        }
        return new StreamingConnection(url, extractAddress(url, PREFIX), prop);
    }
        
	public static synchronized final void initialize(Path dbPath) throws SQLException {
		SyncLite.initialize(DeviceType.STREAMING, dbPath);
	}

	public static synchronized final void initialize(Path dbPath, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.STREAMING, dbPath, deviceName);
	}

	public static synchronized final void initialize(Path dbPath, SyncLiteOptions options) throws SQLException {
		SyncLite.initialize(DeviceType.STREAMING, dbPath, options);
	}

	public static synchronized final void initialize(Path dbPath, SyncLiteOptions options, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.STREAMING, dbPath, options, deviceName);
	}

	public static synchronized final void initialize(Path dbPath, Path propsPath) throws SQLException {
		SyncLite.initialize(DeviceType.STREAMING, dbPath, propsPath);
	}

	public static synchronized final void initialize(Path dbPath, Path propsPath, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.STREAMING, dbPath, propsPath, deviceName);
	}
    
    @Override
    protected String getPrefix() {
    	return PREFIX;
    }

	protected DBProcessor getDBProcessor() {
		return new StreamingProcessor();
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
		options.SetDeviceType(DeviceType.STREAMING);
	}

	@Override
	protected void getOrCreateLoggerInstace(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		//Use SyncEventLogger for this device.
		SyncEventLogger.getInstance(dbPath, options, tracer);
	}	

	@Override
	protected boolean requiresMetadataFile() {
		return false;
	}

}
