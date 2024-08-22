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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class StreamingConnection extends TelemetryConnection {

	public static final String PREFIX = "jdbc:synclite_streaming:";
	protected PreparedStatement nativeCommitLoggerPStmt;
	private SQLStager cmdStager;
	private static final Object commitLock = new Object();
	private static final String insertCommitLoggerSql = "INSERT INTO synclite_txn(commit_id, operation_id) VALUES(?, ?)";


	public StreamingConnection(String url, String fileName, Properties prop) throws SQLException {
		super(url, fileName, prop);
		this.cmdStager = new EventSQLStager(Path.of(fileName), this.sqlLogger.options, commitId);
	}

	protected void prepareCommitLoggerPStmt() throws SQLException {
		//this.nativeCommitLoggerPStmt = duckDBConnection.prepareStatement(commitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
		this.nativeCommitLoggerPStmt = super.prepareUnloggedStatement(insertCommitLoggerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);    	
	}

	SQLStager getCommandStager() {
		return this.cmdStager;
	}

	@Override
	public void commit() throws SQLException {
		synchronized(commitLock) {   		
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
			super.superCommit();

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
			super.superRollback();
			this.commitId = this.sqlLogger.getNextCommitID();
			this.cmdStager = new EventSQLStager(this.path, this.sqlLogger.options, commitId);
		}
	}

	@Override
	final protected void recordCommit() throws SQLException {
		nativeCommitLoggerPStmt.setLong(1, commitId);
		nativeCommitLoggerPStmt.setLong(2, this.sqlLogger.getOperationID());
		nativeCommitLoggerPStmt.execute();
	}    

	@Override
	final protected void initDevice(Properties prop) throws SQLException {
		Object configPathObj = prop.get("config");
		Object deviceName = prop.get("device-name");
		if (configPathObj != null) {
			//Try initializing
			if (deviceName != null) {
				Streaming.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
			} else {
				Streaming.initialize(this.path, Path.of(configPathObj.toString()));
			}
		} else {
			//Try initializing without configs.
			if (deviceName != null) {
				Streaming.initialize(this.path, deviceName.toString());
			} else {
				Streaming.initialize(this.path);
			}
		}
	}

	protected void initDeviceWithoutProps() throws SQLException {
		Streaming.initialize(this.path);
	}


	@Override
	final public void close() throws SQLException {   	
		if (this.cmdStager != null) {
			this.cmdStager.cleanup();
		}
		if (nativeCommitLoggerPStmt != null) {
			nativeCommitLoggerPStmt.close();
		}
		super.close();
	}

	protected Statement connCreateStatement() throws SQLException {
		return new StreamingStatement(this);
	}

	protected PreparedStatement connPrepareStatement(String sql) throws SQLException {
		return new StreamingPreparedStatement(this, sql);
	}

}
