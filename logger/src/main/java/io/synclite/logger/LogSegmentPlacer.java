package io.synclite.logger;

import java.nio.file.Path;

public abstract class LogSegmentPlacer {

	protected abstract Path getLogSegmentPath(Path dbPath, long dbID, long seqNum);
	protected abstract Path getDataFilePath(Path dbPath, long dbID, long seqNum);
	protected abstract Path getTxnStageFilePath(Path dbPath, long txnID);
	protected abstract boolean isTxnFileForLogSegment(long logSeqNum, Path p);
	
}
