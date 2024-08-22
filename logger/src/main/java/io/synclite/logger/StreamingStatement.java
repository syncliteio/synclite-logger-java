package io.synclite.logger;

import java.sql.SQLException;

import org.sqlite.SQLiteConnection;

public class StreamingStatement extends TelemetryStatement {

	protected StreamingStatement(SQLiteConnection conn) throws SQLException {
		super(conn);
	}

	@Override
    final protected StreamingConnection getConn() {
        return ((StreamingConnection ) this.conn);
    }

	private SQLStager getCommandStager() {
		return getConn().getCommandStager();
	}

	@Override
	protected final void logOper(String sql, Object[] args) throws SQLException {
		long commitId = getConn().getCommitId();
		getCommandStager().log(commitId, sql, args);
	}
}
