package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

final class BackupAgentMultiDest extends BackupAgent {

	public BackupAgentMultiDest(Path dbPath, String writeArchieveName, String dataBackupSuffix, MetadataManager metadataMgr, SyncLiteOptions options, Logger tracer) throws SQLException {
		super(dbPath, writeArchieveName, dataBackupSuffix, metadataMgr, options, tracer);
	}

	private final Void doShip(Integer destIndex) {
		//TODO: Implement resumability to avoid re-copy to all destinations post a restart etc.
		//Ship data backup
		copyDataBackup(destIndex);
		//Backup metadata file
		backupMetadataFile(destIndex);
		//Update backup_shipped property in metadata file backup
		updateMetadataBackupProperty(destIndex, "backup_shipped", 1L);
		//Ship metadata file backup
		moveMetadataFileBackup(destIndex);

		return null;
	}


	@Override
	protected final void ship() {
		while (!Thread.interrupted()) {
			try {
				ExecutorService fixedPoolExecutor = Executors.newFixedThreadPool(options.getNumDestinations());
				List<Future<Void>> futureList = new ArrayList<>();

				for (Integer i = 1; i <= options.getNumDestinations(); ++i) {
					final Integer destIndex = i;
					Future<Void> future = fixedPoolExecutor.submit(() -> doShip(destIndex));
					futureList.add(future);
				}

				for (Future<Void> future : futureList) {
					future.get();
				}
				break; 
			} catch (Exception e) {
				tracer.error("SyncLite multidest backup agent failed to ship backup with exception : ", e);
				try {
					Thread.sleep(RETRY_INTERVAL);				
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
				continue;
			}
		}
	}

}
