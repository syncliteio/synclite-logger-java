package io.synclite.logger;

import java.sql.SQLException;

import org.sqlite.SQLiteConnection;

public class StreamingPreparedStatement extends TelemetryPreparedStatement {

	public StreamingPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
		super(conn, sql);
	}

	@Override
    final protected StreamingConnection getConn() {
        return ((StreamingConnection ) this.conn);
    }

	private SQLStager getCommandStager() {
		return getConn().getCommandStager();
	}

	@Override
	protected final void log(Object[] args) throws SQLException {
		long commitId = ((TelemetryConnection ) this.conn).getCommitId();
		if (batchQueryCount == 0) {			
			getCommandStager().log(commitId, this.sql, args);
		} else if (batchQueryCount == 1){
			getCommandStager().log(commitId, this.sql, args);
		} else {
			getCommandStager().log(commitId, null, args);
		}
	}
}
