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
