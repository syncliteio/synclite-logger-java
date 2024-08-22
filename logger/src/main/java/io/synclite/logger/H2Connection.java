package io.synclite.logger;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class H2Connection extends MultiWriterDBConnection {		
    public static final String PREFIX = "jdbc:synclite_h2:";
    private String h2URL;
    protected Connection h2Connection;

	public H2Connection(String url, String fileName, Properties props) throws SQLException {
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
    			H2.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
    		} else {
    			H2.initialize(this.path, Path.of(configPathObj.toString()));
    		}
    	} else {
    		//Try initializing without configs.
    		if (deviceName != null) {
    			H2.initialize(this.path, deviceName.toString());
    		} else {
    			H2.initialize(this.path);
    		}
    	}
	}
    
    @Override
	protected void initDeviceWithoutProps() throws SQLException {
		H2.initialize(this.path);		
	}


    @Override
    final protected void doInitConn() throws SQLException  {
    	try {
    		Class.forName("org.h2.Driver");
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
