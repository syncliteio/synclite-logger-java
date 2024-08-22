package io.synclite.logger;

import java.util.concurrent.CountDownLatch;

class FlushLogRecord extends CommandLogRecord {

	FlushLogRecord(long commitId) {
		super(commitId, null, null);
        CountDownLatch[] latches = new CountDownLatch[1];
        latches[0] = new CountDownLatch(1);
        this.args = latches;		
	}


	final void waitForFlush() throws InterruptedException {
        CountDownLatch isFlushed = (CountDownLatch) args[0];
        isFlushed.await();
    }

    final void setFlushed() {
        CountDownLatch isFlushed = (CountDownLatch) args[0];
        isFlushed.countDown();
    }

}
