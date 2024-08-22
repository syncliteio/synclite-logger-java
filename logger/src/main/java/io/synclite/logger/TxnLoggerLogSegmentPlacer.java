package io.synclite.logger;

import java.nio.file.Path;

public class TxnLoggerLogSegmentPlacer extends LogSegmentPlacer {

	public TxnLoggerLogSegmentPlacer() {
	}

	@Override
	protected Path getLogSegmentPath(Path dbPath, long dbID, long seqNum) {
		return SQLite.getLogSegmentPath(dbPath, dbID, seqNum);
	}

	@Override
	protected Path getDataFilePath(Path dbPath, long dbID, long seqNum) {
		return SQLite.getDataFilePath(dbPath, dbID, seqNum);
	}

	@Override
	protected Path getTxnStageFilePath(Path dbPath, long txnID) {
		return SQLite.getTxnStageFilePath(dbPath, txnID);
	}
	
	protected boolean isTxnFileForLogSegment(long logSeqNum, Path p) {
		return SQLite.isTxnFileForLogSegment(logSeqNum, p);
	}

}
