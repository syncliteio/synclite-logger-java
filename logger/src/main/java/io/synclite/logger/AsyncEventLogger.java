package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class AsyncEventLogger extends EventLogger {

	private BlockingQueue<CommandLogRecord> logQueue;
	protected AsyncEventLogger(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		super(dbPath, options, tracer);
	}

	
	static final EventLogger getInstance(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		if (dbPath == null) {
			return null;
		}
		return (EventLogger) loggers.computeIfAbsent(dbPath, s -> {
			try {
				return new AsyncEventLogger(s, options, tracer);
			} catch (SQLException e) {
				tracer.error("Failed to create/get a SQL logger instance for device : " + dbPath, e);
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	protected void initLogger() {
		initLogQueue();
		startLogging();
	}

	private final void initLogQueue() {
		this.logQueue = new LinkedBlockingQueue<CommandLogRecord>(options.getLogQueueSize());
	}


	@Override
	void log(long commitId, String sql, Object[] args) throws SQLException {
		try {
			logQueue.put(new CommandLogRecord(commitId, sql, args));
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	@Override
	void flush(long commitId) throws SQLException {
		FlushLogRecord flushRecord = new FlushLogRecord(commitId);
		try {
			logQueue.put(flushRecord);
			flushRecord.waitForFlush();
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}

	@Override
	protected void terminateInternal() {
		//If thread is running then terminate
		if (this.isAlive()) {
			try {
				//Wait until queue is drained
				while(!logQueue.isEmpty()) {
					Thread.sleep(1000);
				}
				interrupt();
				join();
			} catch (InterruptedException e) {
				stop();
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
					if (currentBatchLogCount > 0) {
						executeLogBatch();
						commitLogSegment();				
					}
					currentBatchLogCount = 0;
					currentTxnLogCount = 0;
					flushRecord.setFlushed();
					checkups();
					continue;
				}
				//if (currentTxnLogCount == 0) {
				if (this.currentTxnCommitId < record.commitId) {
					//Clear the batch
					//Reset counters of previous transaction
					clearLogBatch();
					this.currentBatchLogCount = 0;
					this.currentTxnLogCount = 0;
				}
				appendLogRecord(record);
			} catch (InterruptedException e) {
				try {
					checkups();
					closeCurrentLogSegment();					
					break;
				} catch (SQLException e1) {
					tracer.error("Closing log segment for logger for device at " + dbPath + " failed with exception : " +  e1);
					break;
				}
			} catch (Exception e) {
				tracer.error("SyncLite Event Logger failed with exception : " +  e);
				isHealthy.set(false);
				break;
			}
		}
	}

}
