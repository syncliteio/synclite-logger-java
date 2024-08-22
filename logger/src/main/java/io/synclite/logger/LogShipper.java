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

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

class LogShipper {

    protected final ConcurrentHashMap<String, Long> clientCommands = new ConcurrentHashMap<String, Long>();
    protected final Path dbPath;
    protected final Path syncLiteDirPath;
    protected final long databaseID;
    protected final String writeArchieveName;
    protected final Path writeArchievePath;
    protected final FSArchiver archiver;
    protected final MetadataManager metadataMgr;
    protected AtomicLong shippedLogSegmentSequenceNumber = new AtomicLong(-1L);
    protected AtomicLong shippedDataFileSequenceNumber = new AtomicLong(-1L);
    protected AtomicLong logSegmentSequenceNumber = new AtomicLong(-1);
    protected AtomicLong dataFileSequenceNumber = new AtomicLong(-1);
    protected final LogSegmentPlacer logSegmentPlacer;
    protected final SyncLiteOptions options;
    protected final ScheduledExecutorService shipperService;
    protected final Integer destIndex;
    protected final Logger tracer;
    private boolean copyTxnFiles = false;

    LogShipper(Path dbPath, long databaseID, String writeArchieveName, LogSegmentPlacer logSegmentPlacer, MetadataManager metadataMgr, SyncLiteOptions options, Integer destIndex, Logger tracer) throws SQLException {
    	this.destIndex = destIndex;
        this.dbPath = dbPath;
        this.syncLiteDirPath = Path.of(this.dbPath + ".synclite");
        this.databaseID = databaseID;
        this.writeArchieveName = writeArchieveName;
        this.metadataMgr = metadataMgr;
        this.writeArchievePath = Path.of(options.getLocalDataStageDirectory(destIndex).toString(), this.writeArchieveName + "/");
        this.tracer = tracer;
        this.logSegmentPlacer = logSegmentPlacer;
        this.options = options;
        this.copyTxnFiles = SyncLiteUtils.deviceAllowsConcurrentWriters(this.options.getDeviceType());
        switch (options.getDestinationType(destIndex)) {
        case FS:
        case MS_ONEDRIVE:
        case GOOGLE_DRIVE:
            this.archiver = new FSArchiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, tracer, options.getEncryptionKeyFile());
            break;
        case SFTP:
            this.archiver = new SFTPArchiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, options.getHost(destIndex), options.getPort(destIndex), options.getUserName(destIndex), options.getPassword(destIndex), options.getRemoteDataStageDirectory(destIndex), null, tracer, options.getEncryptionKeyFile());
            break;
        case MINIO:
            this.archiver = new MinioArchiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, options.getHost(destIndex), options.getUserName(destIndex), options.getPassword(destIndex), options.getRemoteDataStageDirectory(destIndex), null, tracer, options.getEncryptionKeyFile());
            break;     
        case KAFKA:
			this.archiver = new KafkaArchiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, options.getKafkaProducerProperties(destIndex), null, "log", tracer, options.getEncryptionKeyFile());
			break;
        case S3:
        	this.archiver = new S3Archiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, options.getHost(destIndex), options.getUserName(destIndex), options.getPassword(destIndex), options.getRemoteDataStageDirectory(destIndex), null, tracer, options.getEncryptionKeyFile());
        	break;
        default:
            throw new RuntimeException("Unsupported destination type : " + options.getLocalDataStageDirectory(destIndex));
        }

        Long longVal = metadataMgr.getLongProperty("shipped_log_segment_sequence_number-" + destIndex);
        if (longVal != null) {
            this.shippedLogSegmentSequenceNumber.set(longVal);
        } else {
            this.shippedLogSegmentSequenceNumber.set(-1L);
            metadataMgr.insertProperty("shipped_log_segment_sequence_number-" + destIndex, this.shippedLogSegmentSequenceNumber);
        }

        longVal = metadataMgr.getLongProperty("shipped_data_file_sequence_number-" + destIndex);
        if (longVal != null) {
            this.shippedDataFileSequenceNumber.set(longVal);
        } else {
            this.shippedDataFileSequenceNumber.set(-1L);
            metadataMgr.insertProperty("shipped_data_file_sequence_number-" + destIndex, this.shippedDataFileSequenceNumber);
        }

        shipperService = Executors.newScheduledThreadPool(1);
        shipperService.scheduleAtFixedRate(this::ship, 0, options.getLogSegmentShippingFrequencyMs(), TimeUnit.MILLISECONDS);
    }

    Long getShippedLogSegmentSequenceNumber() {
    	return this.shippedLogSegmentSequenceNumber.get();
    }

    Long getShippedDataFileSequenceNumber() {
    	return this.shippedDataFileSequenceNumber.get();
    }

    final void stop() throws InterruptedException {
        if (shipperService != null) {
            shipperService.shutdownNow();
            shipperService.awaitTermination(options.getLogSegmentShippingFrequencyMs(), TimeUnit.MILLISECONDS);
        }
    }

    final void setLogSegmentSequenceNumber(AtomicLong logSegmentSequenceNumber) {
        this.logSegmentSequenceNumber.set(logSegmentSequenceNumber.get());
    }

    final void setDataFileSequenceNumber(AtomicLong dataFileSequenceNumber) {
        this.dataFileSequenceNumber.set(dataFileSequenceNumber.get());
    }

    protected void doShip(Path logFilePath) throws SQLException {
    	archiver.copyToWriteArchive(logFilePath, logFilePath.getFileName().toString());
    }
    
    protected void doClean(Path logFilePath) throws SQLException {
    	//Nothing to do here as cleanup will be done by separate LogCleaner with multiple destinations.
    }
    
    public final void ship() {
        try {
        	//Shipping data files using archiver to write archive
            long currentDataFileSequenceNumber = this.dataFileSequenceNumber.get();
            long currentShippedDataFileSequenceNumber = this.shippedDataFileSequenceNumber.get();
            boolean moved = false;
            long shippedUpto = -1;
            for (long i = currentShippedDataFileSequenceNumber + 1; i <= currentDataFileSequenceNumber; ++i) {
                Path dataFilePath = logSegmentPlacer.getDataFilePath(this.dbPath, this.databaseID, i);
                doShip(dataFilePath);
                moved = true;
                shippedUpto = i;
            }
            if (moved) {
                metadataMgr.updateProperty("shipped_data_file_sequence_number-" + destIndex, shippedUpto);
                this.shippedDataFileSequenceNumber.set(shippedUpto);
                
                //Cleanup shipped data files
                for (long i = currentShippedDataFileSequenceNumber + 1; i <= shippedUpto; ++i) {
                    Path dataFilePath = logSegmentPlacer.getDataFilePath(this.dbPath, this.databaseID, i);
                	doClean(dataFilePath);
                }                
            }
        } catch (Exception e) {
        	tracer.error("SyncLite LogShipper failed to ship data files with exception : " + e.getMessage(), e);
        	//We will keep retrying
        }
        
        
    	try {
        	//Shipping log segments using archiver to write archive
            long currentLogSegmentSequenceNumber = this.logSegmentSequenceNumber.get();
            long currentShippedLogSegmentSequenceNumber = this.shippedLogSegmentSequenceNumber.get();
            boolean moved = false;
            long shippedUpto = -1;
            for (long i = currentShippedLogSegmentSequenceNumber + 1; i < currentLogSegmentSequenceNumber; ++i) {
            	//Copy TxnFiles ahead of log file 
                if (copyTxnFiles) {
                	try (DirectoryStream<Path> stream = Files.newDirectoryStream(syncLiteDirPath)) {
                        for (Path entry : stream) {
                        	if (logSegmentPlacer.isTxnFileForLogSegment(i, entry)) {
                                doShip(entry);
                            }
                        }
                	}
                }
            	Path logFilePath = logSegmentPlacer.getLogSegmentPath(this.dbPath, this.databaseID, i);
                doShip(logFilePath);
                moved = true;
                shippedUpto = i;
            }
            if (moved) {
                metadataMgr.updateProperty("shipped_log_segment_sequence_number-" + destIndex, shippedUpto);
                this.shippedLogSegmentSequenceNumber.set(shippedUpto);

                //Cleanup shipped log files
                for (long i = currentShippedLogSegmentSequenceNumber + 1; i <= shippedUpto; ++i) {
                	try (DirectoryStream<Path> stream = Files.newDirectoryStream(syncLiteDirPath)) {
                        for (Path entry : stream) {
                            if (entry.getFileName().toString().startsWith(String.valueOf(i) + ".")  && entry.getFileName().toString().endsWith(".txn")) {
                                doClean(entry);
                            }
                        }
                	}                	
                	Path logFilePath = logSegmentPlacer.getLogSegmentPath(this.dbPath, this.databaseID, i);
                	doClean(logFilePath);
                }
            }
        } catch (Exception e) {
        	tracer.error("SyncLite LogShipper failed to ship logs with exception : " + e.getMessage(), e);
        	//We will keep retrying
        }
    }

    final void terminate() {
    	if ((shipperService != null) && (!shipperService.isTerminated())) {
    		shipperService.shutdown();
    		try {
    			shipperService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    		} catch (InterruptedException e) {
    			//Ignore
    		}
    		//Terminate archivers
    		
    		if (archiver != null) {
    			archiver.terminate();
    		}
    		//Do one more attempt ship any ready outstanding log segments.
    		ship();
    	}
    }

    final ConcurrentHashMap<String, Long> getClientCommands() {
        return this.clientCommands;
    }
}
