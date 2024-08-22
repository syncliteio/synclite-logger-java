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
