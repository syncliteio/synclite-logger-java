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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class LogCleaner {

	private final Path dbPath;
	private final long databaseID;
	private final List<LogShipper> logShippers;
	private final AtomicLong cleanedLogSegmentSequenceNumber = new AtomicLong(-1);
	private final AtomicLong cleanedDataFileSequenceNumber = new AtomicLong(-1);
	private final ScheduledExecutorService cleanerService;
	private final MetadataManager metadataMgr;
	private final LogSegmentPlacer logSegmentPlacer;
	private final Logger tracer;

	LogCleaner(Path dbPath, long databaseID, List<LogShipper> logShippers, LogSegmentPlacer logSegmentPlacer, MetadataManager metadataMgr, SyncLiteOptions options, Logger tracer) throws SQLException  {
		this.dbPath = dbPath;
		this.databaseID = databaseID;
		this.logShippers = logShippers;
		this.logSegmentPlacer = logSegmentPlacer;
		this.metadataMgr = metadataMgr;
		this.tracer = tracer;

		cleanerService = Executors.newScheduledThreadPool(1);
		cleanerService.scheduleAtFixedRate(this::clean, 0, options.getLogSegmentShippingFrequencyMs(), TimeUnit.MILLISECONDS);

		Long longVal = metadataMgr.getLongProperty("cleaned_log_segment_sequence_number");
		if (longVal != null) {
			this.cleanedLogSegmentSequenceNumber.set(longVal);
		} else {
			this.cleanedLogSegmentSequenceNumber.set(-1L);
			metadataMgr.insertProperty("cleaned_log_segment_sequence_number", this.cleanedLogSegmentSequenceNumber);
		}

		longVal = metadataMgr.getLongProperty("cleaned_data_file_sequence_number");
		if (longVal != null) {
			this.cleanedDataFileSequenceNumber.set(longVal);
		} else {
			this.cleanedDataFileSequenceNumber.set(-1L);
			metadataMgr.insertProperty("cleaned_data_file_sequence_number", this.cleanedDataFileSequenceNumber);
		}

	}

	private void clean() {
		//Find minimum shipped log position across all shippers
		try {
			long cleanLogsUpto = Long.MAX_VALUE;
			for (LogShipper logShipper : logShippers) {
				long logShipperShippedUpto = logShipper.getShippedLogSegmentSequenceNumber();
				if (logShipperShippedUpto < cleanLogsUpto) {
					cleanLogsUpto = logShipperShippedUpto;
				}
			}

			boolean cleaned = false;
			for (long seq = cleanedLogSegmentSequenceNumber.get() + 1; seq <= cleanLogsUpto; ++seq) {
				Path logFilePath = logSegmentPlacer.getLogSegmentPath(this.dbPath, this.databaseID, seq);
				if (Files.exists(logFilePath)) {
					Files.delete(logFilePath);
				}
				cleaned = true;
			}

			if (cleaned) {
				metadataMgr.updateProperty("cleaned_log_segment_sequence_number", cleanLogsUpto);
				this.cleanedLogSegmentSequenceNumber.set(cleanLogsUpto);
			}
		} catch (Exception e) {
			tracer.error("SyncLite LogCleaner failed cleaning up logs with exception : ", e);			
		}
		

		//Find minimum shipped data file across all shippers
		try {
			long cleanDataFilesUpto = Long.MAX_VALUE;
			for (LogShipper logShipper : logShippers) {
				long logShipperShippedUpto = logShipper.getShippedDataFileSequenceNumber();
				if (logShipperShippedUpto < cleanDataFilesUpto) {
					cleanDataFilesUpto = logShipperShippedUpto;
				}
			}

			boolean cleaned = false;
			for (long seq = cleanedDataFileSequenceNumber.get() + 1; seq <= cleanDataFilesUpto; ++seq) {
				Path dataFilePath = logSegmentPlacer.getDataFilePath(this.dbPath, this.databaseID, seq);
				if (Files.exists(dataFilePath)) {
					Files.delete(dataFilePath);
				}
				cleaned = true;
			}

			if (cleaned) {
				metadataMgr.updateProperty("cleaned_data_file_sequence_number", cleanDataFilesUpto);
				this.cleanedDataFileSequenceNumber.set(cleanDataFilesUpto);
			}
		} catch (Exception e) {
			tracer.error("SyncLite LogCleaner failed cleaning up data files with exception : ", e);			
		}

		
	}
	
    final void terminate() {
    	if ((cleanerService != null) && (!cleanerService.isTerminated())) {
    		cleanerService.shutdown();
    		try {
    			cleanerService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    		} catch (InterruptedException e) {
    			//Ignore
    		}
    	}
    }
}
