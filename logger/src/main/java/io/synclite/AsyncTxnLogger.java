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
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

final class AsyncTxnLogger extends TxnLogger {

	private BlockingQueue<CommandLogRecord> logQueue;
	private AsyncTxnLogger(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		super(dbPath, options, tracer);
	}

	private final void initLogQueue() {
		this.logQueue = new LinkedBlockingQueue<CommandLogRecord>(options.getLogQueueSize());
	}

	static final AsyncTxnLogger getInstance(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		if (dbPath == null) {
			return null;
		}
		return (AsyncTxnLogger) loggers.computeIfAbsent(dbPath, s -> {
			try {
				return new AsyncTxnLogger(s, options, tracer);
			} catch (SQLException e) {
				tracer.error("Failed to create/get SQL Logger instance for device : " + dbPath, e); 
				throw new RuntimeException(e);
			}
		});
	}


	protected final void logCommitAndFlush(long commitId) throws SQLException {
		try {
			CommitAndFlushLogRecord rec = new CommitAndFlushLogRecord(commitId);
			logQueue.put(rec);
			rec.waitForFlush();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SQLException("Commit interrupted before flush completed", e);
		}
	}

	protected final void logRollbackAndFlush(long commitId) throws SQLException {
		try {
			RollbackAndFlushLogRecord rec = new RollbackAndFlushLogRecord(commitId);
			logQueue.put(rec);
			rec.waitForFlush();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SQLException("Rollback interrupted before flush completed", e);
		}
	}


	@Override
	final void log(long commitId, String sql, Object[] args) throws SQLException {
		/*
        System.out.println(commitId + " : SQL : " + sql);
        if (args != null) {
            System.out.println("Args : ");
            for (int pos=0; pos < args.length; pos++) {
                System.out.println(args[pos] == null ? "null" : args[pos].toString());
            }
        }*/
		try {
			logQueue.put(new CommandLogRecord(commitId, sql, args));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SQLException("Logger interrupted while queuing log record", e);
		}
	}

	@Override
	final void flush(long commitId) throws SQLException {
		FlushLogRecord flushRecord = new FlushLogRecord(commitId);
		try {
			logQueue.put(flushRecord);
			flushRecord.waitForFlush();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SQLException("Logger interrupted while waiting for flush", e);
		}
	}

	@Override
	public final void run() {
		currentBatchLogCount = 0;
		while (!Thread.interrupted()) {
			try {
				CommandLogRecord record= logQueue.poll(options.getLogSegmentSwitchDurationThresholdMs(), TimeUnit.MILLISECONDS);
				if (record == null) {
					checkups();
					continue;
				}
				if (currentBatchLogCount > options.getLogSegmentFlushBatchSize()) {
					executeLogBatch();
					currentBatchLogCount = 0;
				}
				if (record instanceof FlushLogRecord) {
					FlushLogRecord flushRecord = (FlushLogRecord) record;
					if (record instanceof CommitAndFlushLogRecord) {
						if (this.currentTxnCommitId < flushRecord.commitId) {
							// Empty txn (no ops were logged for this commitId).
							// Emit a BEGIN so the COMMIT is properly bracketed.
							this.currentBatchLogCount = 0;
							this.currentTxnLogCount = 0;
							logBeginTran(new CommandLogRecord(flushRecord.commitId, "BEGIN", null));
						}
						logCommitTran(new CommandLogRecord(flushRecord.commitId, "COMMIT", null));
						executeLogBatch();
						commitLogSegment();
						currentBatchLogCount = 0;
						currentTxnLogCount = 0;
						flushRecord.setFlushed();
						checkups();
					} else if (record instanceof RollbackAndFlushLogRecord) {
						if (this.currentTxnCommitId < flushRecord.commitId) {
							this.currentBatchLogCount = 0;
							this.currentTxnLogCount = 0;
							logBeginTran(new CommandLogRecord(flushRecord.commitId, "BEGIN", null));
						}
						logRollbackTran(new CommandLogRecord(flushRecord.commitId, "ROLLBACK", null));
						executeLogBatch();
						commitLogSegment();
						currentBatchLogCount = 0;
						currentTxnLogCount = 0;
						flushRecord.setFlushed();
						checkups();
					} else {
						if (currentBatchLogCount > 0) {
							executeLogBatch();
							commitLogSegment();							
						}
						flushRecord.setFlushed();
					}
					continue;
				}
				//if (currentTxnLogCount == 0) {
				if (this.currentTxnCommitId < record.commitId) {
					//Append a BEGIN record first for this new txn
					//Reset counters for previous transaction
					this.currentBatchLogCount = 0;
					this.currentTxnLogCount = 0;
					logBeginTran(record);
				}
				appendLogRecord(record);
			} catch (InterruptedException e) {
				try {
					// Drain remaining queued records and unblock pending flush waiters
					CommandLogRecord remaining;
					while ((remaining = logQueue.poll()) != null) {
						if (remaining instanceof FlushLogRecord) {
							((FlushLogRecord) remaining).setFlushed();
						}
					}
					checkups();
					closeCurrentLogSegment();
					break;
				} catch (SQLException e1) {
					tracer.error("Closing log segment for logger for device at " + dbPath + " failed with exception : " +  e1);
					break;
				}
			} catch (Exception e) {
				tracer.error("SyncLite Transactional Logger failed with exception : " +  e);
				isHealthy.set(false);
				break;
			}
		}
	}

	@Override
	protected final void undoLogsForCommit(long restartSlaveCommitID) throws SQLException {
		throw new SQLException("Not implemented");
	}


	@Override
	protected void terminateInternal() {
		//If thread is running then terminate
		if (this.isAlive()) {
			try {
				interrupt();
				join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				interrupt(); // Re-signal the logger thread
			}
		} else {
			//Close current log segment
			try {
				closeCurrentLogSegment();
			} catch (SQLException e) {			
				tracer.error("SyncLite log segment log segment could not be closed properly for device " + dbPath + ", failed with exception : " +  e);
			}
		}
	}

	@Override
	protected void startLogging() {
		start();
	}

	@Override
	protected void initLogger() {
		initLogQueue();
		startLogging();
	}
}
