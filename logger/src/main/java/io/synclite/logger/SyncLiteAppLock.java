package io.synclite.logger;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SyncLiteAppLock {
	
	private Connection lock;
	private Path lockFile;
	public SyncLiteAppLock() {
	}
	
	public final void tryLock(Path dbPath) throws SQLException {		
		this.lockFile = Path.of(dbPath.toString() + ".synclite", dbPath.getFileName().toString() + ".lock");
		String lockFileURL = "jdbc:sqlite:" + lockFile;
		try {
			this.lock = DriverManager.getConnection(lockFileURL);
			try (Statement stmt = this.lock.createStatement()) {
	            stmt.executeUpdate("PRAGMA locking_mode = EXCLUSIVE");
	            stmt.executeUpdate("BEGIN EXCLUSIVE");
			}
		} catch (Exception e) {
			throw new SQLException("Failed to lock db file " + dbPath + ". Another application is using this db file");
		}
	}

	public final void release() {
		if (this.lock != null) {
			try {
				this.lock.close();
			} catch (Exception e) {
				//throw e;
			}
		}
	}
}
