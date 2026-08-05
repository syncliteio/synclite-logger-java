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

package io.synclite;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

public final class SyncTxnLogger extends TxnLogger {

	//Serializes every write path (log/flush/commit/rollback/terminate) against the
	//periodic idle-segment switch below. A segment is only ever switched while this
	//lock is held AND currentTxnLogCount == 0, so a BEGIN..COMMIT can never straddle
	//two log segments.
	private final ReentrantLock segmentLock = new ReentrantLock();
	private ScheduledExecutorService idleSegmentSwitchService;

	public SyncTxnLogger(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		super(dbPath, options, tracer);
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
		segmentLock.lock();
		try {
			CommandLogRecord rec = new CommandLogRecord(commitId, sql, args);
			if (currentTxnLogCount == 0) {
				//This is the first log record of the txn
				logBeginTran(rec);
			}
			appendLogRecord(rec);
		} finally {
			segmentLock.unlock();
		}
	}

	@Override
	void flush(long commitId) throws SQLException {
		segmentLock.lock();
		try {
			//Persist the pending DML batch to the current log segment. The txn stays
			//open (no COMMIT marker yet), so we must NOT treat this as a commit
			//boundary here - the segment is only ever switched from
			//logCommitAndFlush()/logRollbackAndFlush() after the marker is written.
			executeLogBatch();
			commitLogSegment();
		} finally {
			segmentLock.unlock();
		}
	}

	@Override
	protected void terminateInternal() {
		//Stop the idle-switch ticker first so it can never touch the segment while we
		//close it below.
		if (idleSegmentSwitchService != null) {
			idleSegmentSwitchService.shutdownNow();
			try {
				idleSegmentSwitchService.awaitTermination(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		segmentLock.lock();
		try {
			checkups();
			closeCurrentLogSegment();
		} catch (SQLException e) {			
			tracer.error("SyncLite log segment could not be closed properly for device " + dbPath + ", failed with exception : " +  e);
		} finally {
			segmentLock.unlock();
		}
	}

	@Override
	protected void logCommitAndFlush(long commitId) throws SQLException {
		segmentLock.lock();
		try {
			if (this.currentTxnCommitId < commitId) {
				// Empty txn: emit BEGIN so COMMIT is always bracketed.
				appendLogRecord(new CommandLogRecord(commitId, "BEGIN", null));
			}
			appendLogRecord(new CommandLogRecord(commitId, "COMMIT", null));
			executeLogBatch();
			commitLogSegment();
			//Reset current txn log count to 0 to mark the commit boundary.
			this.currentTxnLogCount = 0;
			this.currentBatchLogCount = 0;
			//Switch the log segment inline, on the committing thread, right after the
			//COMMIT marker is durably written. A segment is therefore only ever rolled
			//at a commit boundary. This replaces the earlier background
			//segmentCreatorService and its associated txnInProgress race window.
			checkups();
		} finally {
			segmentLock.unlock();
		}
	}

	@Override
	protected void logRollbackAndFlush(long commitId) throws SQLException {
		segmentLock.lock();
		try {
			if (this.currentTxnCommitId < commitId) {
				// Empty txn: emit BEGIN so rollback boundaries are explicit.
				appendLogRecord(new CommandLogRecord(commitId, "BEGIN", null));
			}
			appendLogRecord(new CommandLogRecord(commitId, "ROLLBACK", null));
			executeLogBatch();
			undoLogsForCommit(commitId);
			commitLogSegment();
			this.currentTxnLogCount = 0;
			this.currentBatchLogCount = 0;
			//Switch the log segment inline, on the committing thread, at the commit boundary.
			checkups();
		} finally {
			segmentLock.unlock();
		}
	}

	/**
	 * Periodic idle-segment closer. A bursty writer (e.g. a DBLOGGER device fed by
	 * dbreader) commits a batch and then goes idle. Because the age-based switch in
	 * checkAndSwitchLogSegment() is only re-evaluated when the next commit arrives,
	 * an idle device would otherwise keep its last segment open forever and it would
	 * never be shipped/consolidated. This tick re-evaluates the switch on a timer.
	 *
	 * Split-safety: we take segmentLock (the same lock every writer path holds) via
	 * tryLock, and only switch when currentTxnLogCount == 0 (a true commit boundary).
	 * If a writer is mid-transaction the lock is held (tryLock fails) or
	 * currentTxnLogCount &gt; 0, so we skip this tick and never split a transaction
	 * across two segments. This is the guarantee the old racing segmentCreatorService
	 * lacked.
	 */
	private void switchIdleLogSegmentIfNeeded() {
		if (!segmentLock.tryLock()) {
			//A writer is active; skip this tick.
			return;
		}
		try {
			if (currentTxnLogCount == 0 && logSegmentLogCount > 0) {
				checkups();
			}
		} catch (SQLException e) {
			tracer.error("SyncLite idle log segment switch failed for device " + dbPath + " with exception : " + e);
			isHealthy.set(false);
		} finally {
			segmentLock.unlock();
		}
	}

	@Override
	protected void startLogging() {
		long intervalMs = options.getLogSegmentSwitchDurationThresholdMs();
		if (intervalMs <= 0) {
			intervalMs = 1000;
		}
		this.idleSegmentSwitchService = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "synclite-idle-segment-switch-" + dbName);
			t.setDaemon(true);
			return t;
		});
		//Use fixed delay (not fixed rate) so a slow switch never causes ticks to pile up.
		this.idleSegmentSwitchService.scheduleWithFixedDelay(this::switchIdleLogSegmentIfNeeded, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void initLogger() {
		startLogging();
	}

}
