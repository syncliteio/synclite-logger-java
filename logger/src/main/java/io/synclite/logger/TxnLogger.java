package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;

import org.apache.log4j.Logger;

abstract class TxnLogger extends SQLLogger {

	protected TxnLogger(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		super(dbPath, options, tracer);
	}

	@Override
	protected final void resolveInDoubtTxn() throws SQLException {
		if (this.restartMasterCommitID > 0) {
			//2PC in doubt resolution
			//Check the end of current log segment
			//- if a commit/rollback is recorded then nothing to do
			//- ELse, record a commit if commitid matches else record a rollback

			if (restartSlaveCommitID == 0) {
				//There is nothing to resolve
				return;
			}
			if (restartSlaveCommitID < restartMasterCommitID) {
				throw new SQLException("Restart recovery failed. Database commit with id : " + restartMasterCommitID + " cannot be larger than last logged commit id : " + restartSlaveCommitID + " in the current log segment : " + logPath);
			} else if (restartSlaveCommitID > restartMasterCommitID) {
				if (restartTxnFate.equalsIgnoreCase("COMMIT")) {
					throw new SQLException("Restart recovery failed. The last logged commit id : " + restartSlaveCommitID + " has a COMMIT fate even when it is not committed in the database : " + dbPath + ". The last committed txn commit id in this database is :" + restartMasterCommitID);
				} else if (restartTxnFate.equalsIgnoreCase("UNKNOWN")) {
					if (this.allowsConcurrentWrites) {
						TxnSQLStager.removeTxnFile(this.dbPath, this.logSegmentSequenceNumber.get(), restartSlaveCommitID);
					}
					logRollbackAndFlush(restartSlaveCommitID);
				}
				//Nothing to do if fate is already ROLLBACK
			} else {
				if (restartTxnFate.equalsIgnoreCase("ROLLBACK")) {
					throw new SQLException("Restart recovery failed. The last logged commit id : " + restartSlaveCommitID + " has a ROLLBACK fate whereas the txn with the same commit id is COMMITTed in the database : " + dbPath);
				} else if (restartTxnFate.equalsIgnoreCase("UNKNOWN")) {
					logCommitAndFlush(restartSlaveCommitID);
				}
				//Nothing to do if fate is already COMMIT
			}
		}
	}

	protected abstract void logCommitAndFlush(long commitId) throws SQLException;

	protected abstract void logRollbackAndFlush(long commitId) throws SQLException;

	protected final void logBeginTran(CommandLogRecord record) throws SQLException {
		appendLogRecord(new CommandLogRecord(record.commitId, "BEGIN", null));
	}

	protected final void logCommitTran(CommandLogRecord record) throws SQLException {
		appendLogRecord(new CommandLogRecord(record.commitId, "COMMIT", null));
	}

	protected final void logRollbackTran(CommandLogRecord record) throws SQLException {
		appendLogRecord(new CommandLogRecord(record.commitId, "ROLLBACK", null));
	}

	@Override
	protected void initializeAdditionalMetadataProperties() throws SQLException {
		String strVal = metadataMgr.getStringProperty("device_type");
		if (strVal == null) {
			metadataMgr.insertProperty("device_type", options.getDeviceType());
		} else if (!SyncLiteUtils.isTransactionalDevice(strVal)) {
			throw new SQLException("SyncLite : This device type : " + strVal + " is not a SyncLite transactional device.");
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
		return SQLite.getLogSegmentSignature();
	}

	@Override
	protected String getMetadtaFileSuffix() {
		return SQLite.getMetadataFileSuffix();
	}

	@Override
	protected Path getMetadataFilePath(Path dbPath) {
		return SQLite.getMetadataFilePath(dbPath);
	}

	@Override
	protected String getWriteArchiveNamePrefix() {
		return SQLite.getWriteArchiveNamePrefix();
	}

	@Override
	protected String getReadArchiveNamePrefix() {
		return SQLite.getReadArchiveNamePrefix();
	}

	@Override
	protected String getDataBackupSuffix() {
		return SQLite.getDataBackupSuffix();
	}

	@Override
	protected long getDefaultPageSize() {
		return 0;
	}

	@Override
	protected LogSegmentPlacer getLogSegmentPlacer() {
		return new TxnLoggerLogSegmentPlacer();		
	}
}
