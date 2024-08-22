package io.synclite.logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

public class TxnSQLStager extends SQLStager {

	TxnSQLStager(Path dbPath, SyncLiteOptions options, long txnID) throws SQLException {
		super(dbPath, options, txnID);
	}

	@Override
	protected void publishTxn(long logSeqNum, long commitID) throws SQLException {
		commit();
		close();
		try {
			Path publishFilePath = SQLite.getTxnFilePath(dbPath, logSeqNum, commitID);
			Files.move(this.txnFilePath, publishFilePath);
		} catch (IOException e) {
			throw new SQLException("Failed to publish transaction file : " + this.txnFilePath  + " for log sequence number :" + logSeqNum + ", commit id : " + commitID + " : " + e.getMessage(), e);
		}
	}
	
	public static void removeTxnFile(Path dbPath, long logSegmentSequenceNumber, long commitID) {
		Path txnFilePath = SQLite.getTxnFilePath(dbPath, logSegmentSequenceNumber, commitID);
		try {
			if (Files.exists(txnFilePath)) {				
				Files.delete(txnFilePath);
			}
		} catch (IOException e) {
			//throw new SQLException("Failed to remove txn File");
		}
	}


}
