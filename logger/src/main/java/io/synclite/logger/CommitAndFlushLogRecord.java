package io.synclite.logger;

public class CommitAndFlushLogRecord extends FlushLogRecord {

	CommitAndFlushLogRecord(long commitId) {
		super(commitId);
	}

}
