package io.synclite.logger;

import java.nio.file.Path;

public class EventLoggerLogSegmentPlacer extends LogSegmentPlacer {

	public EventLoggerLogSegmentPlacer() {
	}

	@Override
	protected Path getLogSegmentPath(Path dbPath, long dbID, long seqNum) {
		return Telemetry.getLogSegmentPath(dbPath, dbID, seqNum);
	}

	@Override
	protected Path getDataFilePath(Path dbPath, long dbID, long seqNum) {
		return Telemetry.getDataFilePath(dbPath, dbID, seqNum);
	}

	@Override
	protected Path getTxnStageFilePath(Path dbPath, long txnID) {
		return Telemetry.getTxnStageFilePath(dbPath, txnID);
	}

	@Override
	protected boolean isTxnFileForLogSegment(long logSeqNum, Path p) {
		return Telemetry.isTxnFileForLogSegment(logSeqNum, p);
	}

}
