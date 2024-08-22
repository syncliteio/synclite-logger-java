package io.synclite.logger;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class HyperSQLAppenderConnection extends MultiWriterDBAppenderConnection {		
    public static final String PREFIX = "jdbc:synclite_hsqldb_appender:";
    private String hsqlDBURL;
    protected Connection hsqlDBConnection;

	public HyperSQLAppenderConnection(String url, String fileName, Properties props) throws SQLException {
		super(url, fileName, props);
	}	
	
    protected void prepareCommitLoggerPStmt() throws SQLException {
    	//this.nativeCommitLoggerPStmt = hsqlDBConnection.prepareStatement(commitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    	this.nativeCommitLoggerPStmt = hsqlDBConnection.prepareStatement(insertCommitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    	
    }

    @Override
	final protected void initDevice(Properties prop) throws SQLException {
    	Object configPathObj = prop.get("config");
    	Object deviceName = prop.get("device-name");
    	if (configPathObj != null) {
    		//Try initializing
    		if (deviceName != null) {
    			HyperSQLAppender.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
    		} else {
    			HyperSQLAppender.initialize(this.path, Path.of(configPathObj.toString()));
    		}
    	} else {
    		//Try initializing without configs.
    		if (deviceName != null) {
    			HyperSQLAppender.initialize(this.path, deviceName.toString());
    		} else {
    			HyperSQLAppender.initialize(this.path);
    		}
    	}
	}
    
    @Override
	protected void initDeviceWithoutProps() throws SQLException {
    	HyperSQLAppender.initialize(this.path);		
	}

    @Override
    final protected void doInitConn() throws SQLException  {
    	try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
    		this.hsqlDBURL = "jdbc:hsqldb:" + this.path.toString();
    		this.hsqlDBConnection = DriverManager.getConnection(hsqlDBURL, this.props);
    		connAutoCommit(false);
    	} catch (Exception e) {
    		throw new SQLException("Failed to connect to the HyperSQL database file : " + this.path + " : " + e.getMessage(), e);
    	}
    }

	final Connection getHyperSQLConnection() {
		return this.hsqlDBConnection;
	}
	
	@Override
	Connection getNativeDBConnection() {
		return this.hsqlDBConnection;
	}

	@Override
	protected void nativeCommit() throws SQLException {
        if (hsqlDBConnection != null) {
        	hsqlDBConnection.commit();
        }
	}

	@Override
	protected void nativeSetAutoCommit(boolean b) throws SQLException {
        if (hsqlDBConnection != null) {
        	hsqlDBConnection.setAutoCommit(b);
        }
	}

	@Override
	protected void nativeRollback() throws SQLException {
        if (hsqlDBConnection != null) {
        	hsqlDBConnection.rollback();
        }
	}

	@Override
	protected PreparedStatement nativeUnloggedPreparedStatement(String sql) throws SQLException {
		return hsqlDBConnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);

	}

	@Override
	protected void nativeCloseCommitLoggerPStmt() throws SQLException {
		if (this.nativeCommitLoggerPStmt != null) {
			this.nativeCommitLoggerPStmt.close();
		}
	}

	@Override
	protected void nativeCloseConnection() throws SQLException {
    	if (this.hsqlDBConnection != null) {
    		if (! this.hsqlDBConnection.isClosed()) {
    			this.hsqlDBConnection.close();
    		}
    	}
	}

	@Override
	protected MultiWriterDBProcessor nativeDBProcessor() {
		return new HyperSQLProcessor();
	}	
}
