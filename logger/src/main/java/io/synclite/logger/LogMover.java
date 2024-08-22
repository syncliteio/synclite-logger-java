package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class LogMover extends LogShipper {

	public LogMover(Path dbPath, long databaseID, String writeArchieveName, LogSegmentPlacer logSegmentPlacer, MetadataManager metadataMgr,
			SyncLiteOptions options, Integer destIndex, Logger tracer) throws SQLException {
		super(dbPath, databaseID, writeArchieveName, logSegmentPlacer, metadataMgr, options, destIndex, tracer);
	}

	@Override
    protected final void doShip(Path logFilePath) throws SQLException {
		archiver.copyToWriteArchive(logFilePath, logFilePath.getFileName().toString());
    }
	
	@Override
    protected final void doClean(Path logFilePath) throws SQLException {
		archiver.deleteLocalObject(logFilePath);
    }
	
}
