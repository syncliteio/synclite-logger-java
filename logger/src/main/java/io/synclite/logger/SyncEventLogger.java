package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public class SyncEventLogger extends EventLogger {
	private ScheduledExecutorService segmentCreatorService;
	private ReentrantLock writeLock;

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
				tracer.error("Failed to create/get a SQL logger instance for device : " + dbPath, e);
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	protected final void checkups() {  	
		try  {
			acquireSegmentLock();
			super.checkups();
			releaseSegmentLock();
		} catch (SQLException e) {
			tracer.error("Failed to perform log segment checkups : ", e);
			throw new RuntimeException("Failed to perform log segment checkups : ", e);
		}		
	}


	private final void acquireSegmentLock() {
		writeLock.lock();
	}

	private final void releaseSegmentLock() {
		if (writeLock.isHeldByCurrentThread()) {
			writeLock.unlock();
		}
	}

	@Override
	void log(long commitId, String sql, Object[] args) throws SQLException {
		if (currentTxnLogCount == 0) {
			//This is the first log record of the txn
			//Lock the log segment
			acquireSegmentLock();
		}
		appendLogRecord(new CommandLogRecord(commitId, sql, args));
	}

	@Override
	void flush(long commitId) throws SQLException {
		executeLogBatch();
		commitLogSegment();
		//Release segment lock 
		releaseSegmentLock();
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
	protected void startLogging() {
	}


	@Override
	protected void initLogger() {
		this.writeLock = new ReentrantLock();
		segmentCreatorService = Executors.newScheduledThreadPool(1);	
	}

}
