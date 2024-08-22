package io.synclite.logger;

import java.sql.SQLException;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4PreparedStatement;

public class SyncLiteAppenderPreparedStatement extends JDBC4PreparedStatement {

	protected SQLLogger sqlLogger;
	protected boolean hasSpecialPositionalArg = false;
	private long processedRowCount = 0;
	protected String tableNameInDDL;

	public SyncLiteAppenderPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
		super(conn, sql);
    	String strippedSql = sql.strip();
    	String tokens[] = strippedSql.split("\\s+");
		if (tokens[0].equalsIgnoreCase("INSERT") && tokens[1].equalsIgnoreCase("INTO")) {
    		SyncLiteUtils.validateInsertForTelemetryAndAppender(strippedSql);
    	} else {
			throw new SQLException("Unsupported SQL : SyncLite appender device does not support SQL : " + sql + ". Supported SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, SELECT");
    	}
		this.sqlLogger = EventLogger.findInstance(getConn().getPath());
		this.tableNameInDDL = SyncLiteUtils.getTableNameFromDDL(sql);
	}

	protected SyncLiteAppenderConnection getConn() {
		return ((SyncLiteAppenderConnection ) this.conn);
	}

	protected void log() throws SQLException {
		Object[] args = new Object[paramCount];
		for (int pos=0; pos < paramCount; pos++) {
			args[pos] = batch[batchPos + pos];
		}
		log(args);
	}

	protected void log(Object[] args) throws SQLException {
		long commitId = ((SyncLiteAppenderConnection ) this.conn).getCommitId();
		if (batchQueryCount == 0) {
			sqlLogger.log(commitId, this.sql, args);
		} else if (batchQueryCount == 1){
			sqlLogger.log(commitId, this.sql, args);
		} else {
			sqlLogger.log(commitId, null, args);
		}
	}

	@Override
	public final boolean execute() throws SQLException {
		int cachedBatchQueryCount = batchQueryCount;
		this.processedRowCount += cachedBatchQueryCount;
		boolean result= pStmtExecute();
		if (cachedBatchQueryCount == 0) {
			log();
		}
		processCommit();
		return result;
	}

	private final void processCommit() throws SQLException {
		if (getConn().getUserAutoCommit() == true) {
			getConn().commit();
		}
	}

	@Override
	public final int[] executeBatch() throws SQLException {
		int cachedBatchQueryCount = batchQueryCount;
		this.processedRowCount += cachedBatchQueryCount;		
		int[] result = pStmtExecuteBatch();
		if (cachedBatchQueryCount == 0) {
			log();
		}
		processCommit();
		return result;
	}

	@Override
	public final void addBatch() throws SQLException {
		pStmtAddBatch();
		log();
	}

	protected boolean pStmtExecute() throws SQLException {
		return superExecute();
	}

	final boolean superExecute() throws SQLException {
		return super.execute();
	}    

	final boolean superExecute(String sql) throws SQLException {
		return super.execute(sql);
	}    

	protected void pStmtAddBatch() throws SQLException {
		super.addBatch();
	}

	protected int[] pStmtExecuteBatch() throws SQLException {
		return super.executeBatch();
	}

	@Override
	public void close() throws SQLException {
		super.close();
	}

}