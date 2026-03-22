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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public abstract class MultiWriterDBAppenderConnection extends SyncLiteAppenderConnection {
	protected PreparedStatement nativeCommitLoggerPStmt;
	protected SQLStager cmdStager;
	protected MultiWriterDBProcessor dbProcessor;
	private static final Object commitLock = new Object();
	protected static final String insertCommitLoggerSql = "INSERT INTO synclite_txn(commit_id, operation_id) VALUES(?, ?)";

	public MultiWriterDBAppenderConnection(String url, String fileName, Properties props) throws SQLException {
		super("jdbc:sqlite:" + Path.of(fileName + ".synclite", Path.of(fileName).getFileName().toString() + ".sqlite").toString(), Path.of(fileName + ".synclite", Path.of(fileName).getFileName().toString() + ".sqlite").toString(), props);
		this.cmdStager = new EventSQLStager(Path.of(fileName), this.sqlLogger.options, commitId);
		this.dbProcessor = nativeDBProcessor();
	}

	abstract MultiWriterDBProcessor nativeDBProcessor();

	SQLStager getCommandStager() {
		return this.cmdStager;
	}

	MultiWriterDBProcessor getDBProcessor() {
		return this.dbProcessor;
	}

	protected void prepareCommitLoggerPStmt() throws SQLException {
		this.nativeCommitLoggerPStmt = nativeUnloggedPreparedStatement(insertCommitLoggerSql);
	}

	@Override
	public void commit() throws SQLException {
		synchronized(commitLock) {

			//Check if cmdStager has accumulated non zero number of logs.
			//
			if (this.cmdStager.getLogSegmentLogCount() > 0) {
				//Generate a fresh commit id     	
				this.commitId = this.sqlLogger.getNextCommitID();

				//log REPLAY_TXN 
				this.sqlLogger.log(commitId, "REPLAY_TXN", null);


				//Publish txn file
				//It is safe to get the current log segment sequence number here as we are in the middle of a txn and hence
				//logger will not switch the log file.
				//
				cmdStager.publishTxn(this.sqlLogger.getCurrentLogSegmentSequenceNumber(), this.commitId);

				//Commit log
				this.sqlLogger.commit(commitId);

				//Record commit of this transaction in user db file.    		
				recordCommit();
			} else {
				cmdStager.cleanup();
			}

			//Commit on user db file
			connCommit();

			//Generate new commit id
			this.commitId = this.sqlLogger.getNextCommitID();

			//Create a new command stager object.
			this.cmdStager = new EventSQLStager(this.path, this.sqlLogger.options, commitId);
		}
	}

	@Override
	public final void rollback() throws SQLException {    	
		synchronized(commitLock) {
			//Delete the txnFile.    	
			this.cmdStager.rollback();
			this.sqlLogger.rollback(commitId);
			connRollback();
			this.commitId = this.sqlLogger.getNextCommitID();
			this.cmdStager = new TxnSQLStager(this.path, this.sqlLogger.options, commitId);
		}
	}

	@Override
	final protected void recordCommit() throws SQLException {
		nativeCommitLoggerPStmt.setLong(1, commitId);
		nativeCommitLoggerPStmt.setLong(2, this.sqlLogger.getOperationID());
		nativeCommitLoggerPStmt.execute();
	}


	@Override
	protected final void initPath(String fileName) {
		//
		//Set the path correctly to specified DuckDB file.
		//
		this.path = Path.of(props.get("original-db-path").toString());
		this.props.remove("original-db-path");
		this.props.remove("original-db-url");
	}

	@Override
	protected abstract void initDevice(Properties prop) throws SQLException;

	@Override
	protected abstract void initDeviceWithoutProps() throws SQLException;


	@Override
	protected abstract void doInitConn() throws SQLException;

	abstract Connection getNativeDBConnection();

	@Override
	final protected void connCommit() throws SQLException {
		super.connCommit();
		nativeCommit();
	}

	protected abstract void nativeCommit() throws SQLException;

	@Override
	final protected void connAutoCommit(boolean b) throws SQLException {
		super.connAutoCommit(b);
		nativeSetAutoCommit(b);
	}

	protected abstract void nativeSetAutoCommit(boolean b) throws SQLException;

	@Override
	final protected void connRollback() throws SQLException {
		super.connRollback();
		nativeRollback();
	}

	protected abstract void nativeRollback() throws SQLException;

	@Override
	protected final Statement connCreateStatement() throws SQLException {
		return new MultiWriterDBAppenderStatement(this);
	}

	@Override
	protected final PreparedStatement connPrepareStatement(SyncLiteAppenderConnection conn, String sql) throws SQLException {
		return new MultiWriterDBAppenderPreparedStatement(this, sql);
	}

	@Override 
	public PreparedStatement prepareUnloggedStatement(String sql) throws SQLException {
		return nativeUnloggedPreparedStatement(sql);
	}

	protected abstract PreparedStatement nativeUnloggedPreparedStatement(String sql) throws SQLException;

	@Override
	final public void close() throws SQLException {
		super.close();

		if (this.cmdStager != null) {
			this.cmdStager.cleanup();
		}
		nativeCloseCommitLoggerPStmt();
		nativeCloseConnection();
	}

	protected abstract void nativeCloseCommitLoggerPStmt() throws SQLException;

	protected abstract void nativeCloseConnection() throws SQLException;

}
