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
