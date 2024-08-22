package io.synclite.logger;

import java.sql.SQLException;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4PreparedStatement;

public class InternalTelemetryPreparedStatement extends JDBC4PreparedStatement {

	private final static String mockSql = "Select 1";
	private final String internalSql;
	public InternalTelemetryPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
		super(conn, mockSql);
		SyncLiteUtils.checkInternalSql(sql);
		this.internalSql = sql;		
	}
	
	@Override
    public final boolean execute() throws SQLException {
    	SyncLiteUtils.checkAndExecuteInternalTelemetrySql(internalSql);
    	return false;
    }
    @Override
    public final int[] executeBatch() throws SQLException {
    	execute();
    	return new int[1];
    }

}
