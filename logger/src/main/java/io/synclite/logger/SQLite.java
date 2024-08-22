package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.sqlite.SQLiteConnection;

public final class SQLite extends SyncLite {
	private final static String PREFIX = "jdbc:synclite_sqlite:";
	
	@Override
    public final SQLiteConnection createSyncLiteConnection(String url, Properties prop) throws SQLException {
        if (!checkDeviceURL(url)) {
            return null;       
        }
        url = url.trim();
        prop.put("original-db-url", url);
        prop.put("original-db-path", extractAddress(url, PREFIX));
        //return new DuckDBConnection(url, extractAddress(url), prop);
    	return new SyncLiteConnection(url, extractAddress(url, PREFIX), prop); 
    }
    
    
	public static synchronized final void initialize(Path dbPath) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE, dbPath);
	}

	public static synchronized final void initialize(Path dbPath, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE, dbPath, deviceName);
	}

	public static synchronized final void initialize(Path dbPath, SyncLiteOptions options) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE, dbPath, options);
	}

	public static synchronized final void initialize(Path dbPath, SyncLiteOptions options, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE, dbPath, options, deviceName);
	}

	public static synchronized final void initialize(Path dbPath, Path propsPath) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE, dbPath, propsPath);
	}

	public static synchronized final void initialize(Path dbPath, Path propsPath, String deviceName) throws SQLException {
		SyncLite.initialize(DeviceType.SQLITE, dbPath, propsPath, deviceName);
	}
    
    @Override
    protected String getPrefix() {
    	return "jdbc:synclite_sqlite:";
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
		options.SetDeviceType(DeviceType.SQLITE);
	}

	@Override
	protected void getOrCreateLoggerInstace(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
    	if (options.getDisableAsyncLoggingForTxnDevice()) {
    		SyncTxnLogger.getInstance(dbPath, options, tracer);
    	} else {
    		AsyncTxnLogger.getInstance(dbPath, options, tracer);    		
    	}
	}	
	
}
