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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public final class SyncTxnLogger extends TxnLogger {

    private ScheduledExecutorService segmentCreatorService;
	private AtomicBoolean txnInProgress = new AtomicBoolean();

	public SyncTxnLogger(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		super(dbPath, options, tracer);
        segmentCreatorService.scheduleAtFixedRate(this::checkups, 0, options.getLogSegmentSwitchDurationThresholdMs(), TimeUnit.MILLISECONDS);
	}

	static final SyncTxnLogger getInstance(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		if (dbPath == null) {
			return null;
		}
		return (SyncTxnLogger) loggers.computeIfAbsent(dbPath, s -> {
			try {
				return new SyncTxnLogger(s, options, tracer);
			} catch (SQLException e) {
				tracer.error("Failed to create/get SQL Logger instance for device : " + dbPath, e); 
				throw new RuntimeException(e);
			}
		});
	}


	@Override
    protected final void checkups() {  	
		try  {
			synchronized (txnInProgress) {
				if (!txnInProgress.get()) {
					super.checkups();
				}
			}
		} catch (SQLException e) {
			tracer.error("SyncLite Logger failed to perform log segment checkups : ", e);
			throw new RuntimeException("SyncLite Logger failed to perform log segment checkups : ", e);
		}		
	}

	@Override
	protected void undoLogsForCommit(long commitId) throws SQLException {
        try (Statement stmt = logTableConn.createStatement()) {
            stmt.executeUpdate("DELETE FROM commandlog WHERE commit_id = " + commitId);
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM commandlog;")) {
                if (rs.next()) {
                    this.logSegmentLogCount = rs.getLong(1);
                }
            }            
        }
	}

	@Override
	void log(long commitId, String sql, Object[] args) throws SQLException {
		CommandLogRecord rec = new CommandLogRecord(commitId, sql, args);
        if (currentTxnLogCount == 0) {
        	//This is the first log record of the txn
			synchronized(txnInProgress) {
				txnInProgress.set(true);
			}
        	logBeginTran(rec);
        }
		appendLogRecord(rec);
	}

	@Override
	void flush(long commitId) throws SQLException {
        executeLogBatch();
        commitLogSegment();
		synchronized (txnInProgress) {
			txnInProgress.set(false);
		}
	}

	@Override
	protected void terminateInternal() {
		try {
			checkups();
			closeCurrentLogSegment();
			stopSegmentCreatorService();
		} catch (SQLException e) {			
			tracer.error("SyncLite log segment log segment could not be closed properly for device " + dbPath + ", failed with exception : " +  e);
		}
	}

	private final void stopSegmentCreatorService() {
    	if ((segmentCreatorService != null) && (!segmentCreatorService.isTerminated())) {
    		segmentCreatorService.shutdown();
    		try {
    			segmentCreatorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    		} catch (InterruptedException e) {
    			//Ignore
    		}
    	}
	}

	@Override
	protected void logCommitAndFlush(long commitId) throws SQLException {
		appendLogRecord(new CommandLogRecord(commitId, "COMMIT", null));
		flush(commitId);
        //Reset current txn log count to 0 to enable log switching on commit boundary
        this.currentTxnLogCount = 0;
        this.currentBatchLogCount = 0;
		checkups();
	}

	@Override
	protected void logRollbackAndFlush(long commitId) throws SQLException {
		undoLogsForCommit(commitId);
		synchronized (txnInProgress) {
			txnInProgress.set(false);
		}
	}

	@Override
	protected void startLogging() {
	}

	@Override
	protected void initLogger() {
        segmentCreatorService = Executors.newScheduledThreadPool(1);
	}

}
