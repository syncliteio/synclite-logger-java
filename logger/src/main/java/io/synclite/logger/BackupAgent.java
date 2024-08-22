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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

class BackupAgent {

	protected static final long RETRY_INTERVAL = 1000;
	protected final Path dbPath;
	protected final String writeArchieveName;
	protected Path writeArchievePath;
	protected final HashMap<Integer, FSArchiver> archivers = new HashMap<Integer, FSArchiver>();

	protected final MetadataManager metadataMgr;
	protected long backupTaken;
	protected final SyncLiteOptions options;
	protected ExecutorService backupExecutor;
	protected long backupShipped;
	//protected SQLLogger sqlLogger;
	protected final String dataBackupSuffix;
	protected final Path dataBackupPath;
	protected final Logger tracer;

	BackupAgent(Path dbPath, String writeArchieveName, String dataBackupSuffix, MetadataManager metadataMgr, SyncLiteOptions options, Logger tracer) throws SQLException {
		//this.sqlLogger = sqlLogger;
		this.options = options;
		this.dbPath = dbPath;
		this.writeArchieveName = writeArchieveName;
		this.dataBackupSuffix = dataBackupSuffix;
		this.dataBackupPath = getDataBackupPath();
		this.metadataMgr = metadataMgr;
		this.tracer = tracer;
		for (Integer i = 1; i <= options.getNumDestinations(); ++i) {
			this.writeArchievePath = Path.of(options.getLocalDataStageDirectory(i).toString(), this.writeArchieveName + "/");			
			FSArchiver archiver;
			switch (options.getDestinationType(i)) {
			case FS:
			case MS_ONEDRIVE:
			case GOOGLE_DRIVE:	
				archiver = new FSArchiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, tracer, options.getEncryptionKeyFile());
				archivers.put(i, archiver);
				break;
			case SFTP:
				archiver = new SFTPArchiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, options.getHost(i), options.getPort(i), options.getUserName(i), options.getPassword(i), options.getRemoteDataStageDirectory(i), null, tracer, options.getEncryptionKeyFile());
				archivers.put(i, archiver);
				break;
			case MINIO:
				archiver = new MinioArchiver(dbPath, this.writeArchieveName, this.writeArchievePath, null,null, options.getHost(i), options.getUserName(i), options.getPassword(i), options.getRemoteDataStageDirectory(i), null, tracer, options.getEncryptionKeyFile());
				archivers.put(i, archiver);
				break;
			case KAFKA:
				archiver = new KafkaArchiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, options.getKafkaProducerProperties(i), null, "data", tracer, options.getEncryptionKeyFile());
				archivers.put(i, archiver);
				break;
	        case S3:
				archiver = new S3Archiver(dbPath, this.writeArchieveName, this.writeArchievePath, null, null, options.getHost(i), options.getUserName(i), options.getPassword(i), options.getRemoteDataStageDirectory(i), null, tracer, options.getEncryptionKeyFile());
	        	archivers.put(i, archiver);
	        	break;				
			default:
				throw new RuntimeException("Unsupported destination type : " + options.getDestinationType(i));
			}
			try {
				//Local archive must be created to move on from this point.
				do {
					archiver.createLocalWriteArchiveIfNotExists();
				} while (!archiver.localArchiveExists());
			} catch (SQLException e) {
				tracer.error("SyncLite backup agent failed to create a local write archive with exception : " + e.getMessage(), e);
			}
		}
		//check if backup was already done
		Long longVal = metadataMgr.getLongProperty("backup_taken");
		if (longVal != null) {
			this.backupTaken = longVal;
		} else {
			this.backupTaken = 0;
			metadataMgr.insertProperty("backup_taken", this.backupTaken);
		}

		backupExecutor = Executors.newSingleThreadExecutor();
		backupExecutor.submit(this::backupAndShip);
	}

	final long getBackupShipped() {
		return this.backupShipped;
	}

	final void stop() throws InterruptedException {
		if (backupExecutor != null) {
			backupExecutor.shutdownNow();
			backupExecutor.awaitTermination(10, TimeUnit.SECONDS);
		}
	}

	private final void doTakeBackup() throws SQLException {
		if (this.backupTaken == 0) {
			if (!options.usePreCreatedDataBackup()) {
				DBProcessor reader = DBProcessor.getInstance(options.getDeviceType());
				reader.backupDB(dbPath, this.dataBackupPath, options, false);
			}			
		}
	}

	protected final void backup() {
		if (this.backupTaken == 0) {
			//Backup database
			takeBackup();
			//Process backup
			processBackup();
			//Update metadata property for backup_taken
			updateMetadataProperty("backup_taken", 1L);
		}		
	}
	
	protected void ship() {
		//Ship data backup
		copyDataBackup(1);
		//Backup metadata file
		backupMetadataFile(1);
		//Update backup_shipped property in metadata file backup
		updateMetadataBackupProperty(1, "backup_shipped", 1L);		
		//Ship metadata file backup
		moveMetadataFileBackup(1);
	}
	
	private final void postShip() {
		//Update backup_shipped status in metadata file
		updateMetadataProperty("backup_shipped", 1L);
		
		//delete shipped data backup file
		deletedShippedDataBackup();		

		//Terminate archivers		
		terminateArchivers();
	}

	private final void deletedShippedDataBackup() {
		while(!Thread.interrupted()) {
			try {
				//Delete shipped data backup file
				if (Files.exists(this.dataBackupPath)) {
					Files.delete(this.dataBackupPath);
				}
				break;
			} catch (Exception e) {
				tracer.error("SyncLite backup agent failed to delete data backup post shipping with exception : ", e);
				break;
			}
		}

	}

	protected final void terminateArchivers() {
		for (FSArchiver a : this.archivers.values()) {
			a.terminate();
		}
	}

	protected void backupAndShip() {
		backup();
		ship();
		postShip();
	}

	private final void doProcessBackup() throws SQLException {
		List<String> includeTables = options.getIncludeTables();
		List<String> excludeTables = options.getExcludeTables();
		if ((includeTables == null) && (excludeTables == null)) {
			return;
		}
		
		DBProcessor reader = DBProcessor.getInstance(options.getDeviceType());		
		reader.processBackupDB(this.dataBackupPath, options);
	}

	final void terminate() {
		if ((backupExecutor != null) && (!backupExecutor.isTerminated())) {
			backupExecutor.shutdown();
			try {
				backupExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			} catch (InterruptedException e) {
				//Ignore
			}
		}
	}

	protected void processBackup() {
		//Move data backup
		while(!Thread.interrupted()) {
			try {
				doProcessBackup();
				break;
			} catch (Exception e) {
				this.tracer.error("SyncLite backup agent failed to process backup with exception : ", e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}
	}

	protected void takeBackup() {
		//Move data backup
		while(!Thread.interrupted()) {
			try {
				doTakeBackup();
				break;
			} catch (Exception e) {
				this.tracer.error("SyncLite backup agent failed to take backup for device : " + this.dbPath + " with exception :", e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}
	}

	protected void moveDataBackup(Integer destIndex) {
		//Move data backup
		while(!Thread.interrupted()) {
			try {
				archivers.get(destIndex).moveToWriteArchive(this.dataBackupPath, this.dataBackupPath.getFileName().toString());
				break;
			} catch (Exception e) {
				this.tracer.error("SyncLite backup agent failed to move data backup " + this.dataBackupPath +" for device " + this.dbPath + " with exception : ", e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}
	}

	protected void copyDataBackup(Integer destIndex) {
		//Move data backup
		while(!Thread.interrupted()) {
			try {
				archivers.get(destIndex).copyToWriteArchive(this.dataBackupPath, this.dataBackupPath.getFileName().toString());
				break;
			} catch (Exception e) {
				this.tracer.error("SyncLite backup agent failed to copy data backup for device " + this.dbPath + " with exception : ", e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}
	}


	protected final void backupMetadataFile(Integer destIndex) {
		//Backup metadata file 
		while(!Thread.interrupted()) {
			try {
				Path metadataFileBackupPath = Path.of(metadataMgr.getMetadataFilePath().toString() + "."  + String.valueOf(destIndex));
				metadataMgr.backupMetadataFile(metadataFileBackupPath);
				break;
			} catch (Exception e) {
				this.tracer.error("SyncLite backup agent failed to backup metadata file " + metadataMgr.getMetadataFilePath() + " with exception : ", e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}

	}

	protected final void updateMetadataBackupProperty(Integer destIndex, String propertyName, long PropertyValue) {
		//Update metadata file backup
		while(!Thread.interrupted()) {
			try {
				Path metadataFileBackupPath = Path.of(metadataMgr.getMetadataFilePath().toString() + "."  + String.valueOf(destIndex));
				//Update backup_shipped property to 1 in metadata file backup    			
				MetadataManager metadataMgr = new MetadataManager(metadataFileBackupPath);
				metadataMgr.updateProperty(propertyName, PropertyValue);
				metadataMgr.close();
				break;
			} catch (Exception e) {
				this.tracer.error("SyncLite backup agent failed to update metadata property in metadata backup file with exception : ", e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}
	}

	protected final void moveMetadataFileBackup(Integer destIndex) {
		//Ship metadata file 
		while(!Thread.interrupted()) {
			try {
				Path metadataFileBackupPath = Path.of(metadataMgr.getMetadataFilePath().toString() + "."  + String.valueOf(destIndex));
				archivers.get(destIndex).moveToWriteArchive(metadataFileBackupPath, metadataMgr.getMetadataFilePath().getFileName().toString());
				break;
			} catch (Exception e) {
				this.tracer.error("SyncLite backup agent failed to move metadata file backup with exception : ", e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}
	}

	protected final void updateMetadataProperty(String propertyName, Long propertyValue) {
		//Ship metadata file 
		while(!Thread.interrupted()) {
			try {
				metadataMgr.updateProperty(propertyName, propertyValue);
				this.backupShipped = 1L;
				break;
			} catch (Exception e) {
				this.tracer.error("SyncLite backup agent failed to update metadata property with exception : ", e);
				try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}
	}	
	
    private final Path getDataBackupPath() {
        return Path.of(dbPath.toString() + ".synclite", dbPath.getFileName().toString() + this.dataBackupSuffix);
    }

}
