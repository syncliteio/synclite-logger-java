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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public abstract class EventLogger extends SQLLogger {

	protected EventLogger(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		super(dbPath, options, tracer);
	}

	@Override
	protected final void resolveInDoubtTxn() throws SQLException {
		if (this.restartMasterCommitID > 0) {
			//2PC in doubt resolution
			//Check the end of current log segment
			//- if a DML was recorded then nothing to be done
			//- If a DDL was recorded then make sure it was executed on master , else rollback from log
			//
			if (restartSlaveCommitID == 0) {
				//There is nothing to resolve
				return;
			}
			if (restartSlaveCommitID < restartMasterCommitID) {
				throw new SQLException("Restart recovery failed. Database commit with id : " + restartMasterCommitID + " cannot be larger than last logged commit id : " + restartSlaveCommitID + " in the current log segment : " + logPath);
			} else if (restartSlaveCommitID > restartMasterCommitID) {
				//Streaming, Telemetry and Appender device always performs a 2PC for all operations. 
				//Hence delete logs for last txn as it was not committed on local/master device.  
				undoLogsForCommit(restartSlaveCommitID);
				if (this.allowsConcurrentWrites) {
					//Delete txn file if present for this commit ID
					EventSQLStager.removeTxnFile(this.dbPath, this.logSegmentSequenceNumber.get(), restartSlaveCommitID);
				}
			} else {                
				//Nothing to do as both restartSlaveCommitID and restartMasterCommitID matched.
			}
		}
	}

	@Override
	protected final void undoLogsForCommit(long commitId) throws SQLException {
		try (Statement stmt = logTableConn.createStatement()) {
			stmt.executeUpdate("DELETE FROM commandlog WHERE commit_id = " + commitId);
			try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM commandlog;")) {
				if (rs.next()) {
					this.logSegmentLogCount = rs.getLong(1);
				}
			}            
		}        
	}

	final void commit(long commitId) throws SQLException {
		flush(commitId);
		//Reset current txn log count to 0 to enable log switching on commit boundary
		this.currentTxnLogCount = 0;
		this.currentBatchLogCount = 0;
		checkups();
	}   

	final void rollback(long commitId) throws SQLException {
		rollbackLogSegment();
		this.currentTxnLogCount = 0;
		this.currentBatchLogCount = 0;
		checkups();
	}   
	
	@Override
	protected void initializeAdditionalMetadataProperties() throws SQLException {
		String strVal = metadataMgr.getStringProperty("device_type");
		if (strVal == null) {
			metadataMgr.insertProperty("device_type", options.getDeviceType().toString());
		} else {
			if (SyncLiteUtils.isTransactionalDevice(strVal)) {
				throw new SQLException("SyncLite : This device type : " + strVal + " is not a SyncLite telemetry, streaming or an appender device. ");
			}
		}
		
		boolean allowsConcurrentWrites = SyncLiteUtils.deviceAllowsConcurrentWriters(options.getDeviceType());
		strVal = metadataMgr.getStringProperty("allow_concurrent_writers");
		if (strVal == null) {
			metadataMgr.insertProperty("allow_concurrent_writers", allowsConcurrentWrites);
		}
		this.allowsConcurrentWrites = allowsConcurrentWrites;
	}
	
	@Override
	protected String getLogSegmentSignature() {
		return Telemetry.getLogSegmentSignature();
	}

	@Override
	protected String getMetadtaFileSuffix() {
		return Telemetry.getMetadataFileSuffix();
	}

	@Override
	protected Path getMetadataFilePath(Path dbPath) {
		return Telemetry.getMetadataFilePath(dbPath);
	}

	@Override
	protected String getWriteArchiveNamePrefix() {
		return Telemetry.getWriteArchiveNamePrefix();
	}

	@Override
	protected String getReadArchiveNamePrefix() {
		return Telemetry.getReadArchiveNamePrefix();
	}

	@Override
	protected String getDataBackupSuffix() {
		return Telemetry.getDataBackupSuffix();
	}

	@Override
	protected long getDefaultPageSize() {
		return 512;
	}

	@Override
	protected LogSegmentPlacer getLogSegmentPlacer() {
		return new EventLoggerLogSegmentPlacer();		
	}

	@Override
	protected void startLogging() {
		start();
	}

	public static void removeTxnFile(Path dbPath, long logSegmentSequenceNumber, long commitID) {
		Path txnFilePath = Telemetry.getTxnFilePath(dbPath, logSegmentSequenceNumber, commitID);
		try {
			Files.delete(txnFilePath);
		} catch (IOException e) {
			//throw new SQLException("Failed to remove txn File");
		}
	}

}
