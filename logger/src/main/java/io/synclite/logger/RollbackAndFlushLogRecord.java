package io.synclite.logger;

public class RollbackAndFlushLogRecord extends FlushLogRecord {

	RollbackAndFlushLogRecord(long commitId) {
		super(commitId);
	}
	
}
