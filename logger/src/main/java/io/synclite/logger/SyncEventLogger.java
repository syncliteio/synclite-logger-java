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
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public class SyncEventLogger extends EventLogger {
	private ScheduledExecutorService segmentCreatorService;
	private AtomicBoolean txnInProgress = new AtomicBoolean(false);
	private SyncEventLogger(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		super(dbPath, options, tracer);
		segmentCreatorService.scheduleAtFixedRate(this::checkups, 0, options.getLogSegmentSwitchDurationThresholdMs(), TimeUnit.MILLISECONDS);
	}

	static final EventLogger getInstance(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		if (dbPath == null) {
			return null;
		}
		return (EventLogger) loggers.computeIfAbsent(dbPath, s -> {
			try {
				return new SyncEventLogger(s, options, tracer);
			} catch (SQLException e) {
				tracer.error("Failed to create/get a SQL logger instance for device : " + dbPath + " : " + e.getMessage(), e);
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	protected final void checkups() {  	
		try {
			synchronized (txnInProgress) {				
				if (!txnInProgress.get()) {
					super.checkups();
				}
			}
		} catch (SQLException e) {
			tracer.error("Failed to perform log segment checkups : " + e.getMessage(), e);
			throw new RuntimeException("Failed to perform log segment checkups : " + e.getMessage(), e);
		}		
	}

	@Override
	void log(long commitId, String sql, Object[] args) throws SQLException {
		if (currentTxnLogCount == 0) {
        	//This is the first log record of the txn
			synchronized(txnInProgress) {
				txnInProgress.set(true);
			}
		}
		appendLogRecord(new CommandLogRecord(commitId, sql, args));
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
	protected void doRollback() throws SQLException {
		rollbackLogSegment();
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
			tracer.error("SyncLite log segment log segment could not be closed properly for device " + dbPath + ", failed with exception : " + e.getMessage(), e);
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
	protected void startLogging() {
	}


	@Override
	protected void initLogger() {
		segmentCreatorService = Executors.newScheduledThreadPool(1);	
	}

}
