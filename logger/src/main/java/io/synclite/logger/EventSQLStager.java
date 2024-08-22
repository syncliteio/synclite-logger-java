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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

public class EventSQLStager extends SQLStager {

	EventSQLStager(Path dbPath, SyncLiteOptions options, long txnID) throws SQLException {
		super(dbPath, options, txnID);
	}

	@Override
	protected void publishTxn(long logSeqNum, long commitID) throws SQLException {
		commit();
		close();
		try {
			Path publishFilePath = Telemetry.getTxnFilePath(dbPath, logSeqNum, commitID);
			Files.move(this.txnFilePath, publishFilePath);
		} catch (IOException e) {
			throw new SQLException("Failed to publish transaction file : " + this.txnFilePath  + " for log sequence number :" + logSeqNum + ", commit id : " + commitID + " : " + e.getMessage(), e);
		}
	}
	
	public static void removeTxnFile(Path dbPath, long logSegmentSequenceNumber, long commitID) {
		Path txnFilePath = Telemetry.getTxnFilePath(dbPath, logSegmentSequenceNumber, commitID);
		try {
			Files.delete(txnFilePath);
		} catch (IOException e) {
			//throw new SQLException("Failed to remove txn File");
		}
	}


}
