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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

abstract class SQLLogger extends Thread {

	protected static final ConcurrentHashMap<Path, SQLLogger> loggers = new ConcurrentHashMap<Path, SQLLogger>();    
	protected static final String createLogTableSqlTemplate = "CREATE TABLE IF NOT EXISTS commandlog(change_number INTEGER PRIMARY KEY, commit_id LONG, sql TEXT, arg_cnt INTEGER, $1);";
	protected static final String insertLogTableSqlTemplate = "INSERT INTO commandlog(change_number, commit_id, sql, arg_cnt, $1) VALUES ($2)";
	protected static final String alterLogTableSqlTemplate = "ALTER TABLE commandlog ADD COLUMN $1";
	protected static final String createTxnTableSql = "CREATE TABLE IF NOT EXISTS synclite_txn(commit_id long, operation_id long);";
	protected static final String dropTxnTableSql = "DROP TABLE IF EXISTS synclite_txn;";
	protected static final String selectTxnTableSql = "SELECT commit_id, operation_id FROM synclite_txn;";
	protected static final String insertTxnTable = "INSERT INTO synclite_txn VALUES(0,0);";
	protected static final String dropMetadataTableSql = "DROP TABLE IF EXISTS metadata";
	protected static final String createMetadataTableSql = "CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT)";
	protected static final String insertMetadataTableSql = "INSERT INTO metadata(key, value) VALUES ('status', 'NEW')";
	protected static final String updateMetadataTableSql = "UPDATE metadata SET value = '" + LogSegmentStatus.READY_TO_APPLY + "' WHERE key = 'status'" ; 
	protected Connection logTableConn = null;
	protected PreparedStatement insertLogTablePstmt = null;
	protected Set<PreparedStatement> additionalPrepStmts = new HashSet<PreparedStatement>();
	protected Map<Long, PreparedStatement> argTablePrepStmtsMap = new HashMap<Long, PreparedStatement>();
	protected Path dbPath;
	protected Path dbName;
	protected Path logPath;
	protected Path metadataFilePath;
	protected UUID uuid;
	protected String deviceName;
	protected AtomicLong logSegmentSequenceNumber = new AtomicLong(-1);
	protected AtomicLong dataFileSequenceNumber = new AtomicLong(-1);
	protected long logSegmentLogCount;
	protected long currentTxnCommitId;
	protected long currentTxnLogCount;
	protected long currentBatchLogCount;
	protected long lastLogSegmentCreateTime;
	protected long restartMasterCommitID;
	protected long restartSlaveCommitID;
	protected long currentOperationId;
	protected String restartTxnFate;
	protected String restartLoggedSQL;
	protected long backupShipped;
	protected long databaseID;
	protected long lastProcessedRequestID;
	protected long lastProcessingRequestID;
	protected String lastProcessingCommand;
	protected boolean allowsConcurrentWrites;
	protected MetadataManager metadataMgr;
	protected BackupAgent backupAgent;
	protected List<LogShipper> logShippers = new ArrayList<LogShipper>();
	protected List<CommandHandler> cmdHandlers = new ArrayList<CommandHandler>();
	protected LogCleaner logCleaner;
	protected Long inlinedArgCnt;
	protected SyncLiteOptions options;
	protected Logger tracer;
	protected LogSegmentPlacer logSegmentPlacer;
	private boolean terminateInProgress;
	protected AtomicBoolean isHealthy = new AtomicBoolean(true);
	private SyncLiteAppLock appLock = new SyncLiteAppLock();
	private static AtomicLong latestGeneratedCommitId = new AtomicLong(System.currentTimeMillis());

	protected SQLLogger(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
		this.options = options;
		this.dbPath = dbPath;
		this.dbName = dbPath.getFileName();
		this.tracer = tracer;
		try {
			tryLockDB();
			initLogger();
			initializeMetadataProperties();
			this.logSegmentPlacer = getLogSegmentPlacer();
			restartRecovery();
			initializeLogShipper();
			initializeCommandHandlers();
			initializeBackupAgent();
		} catch (SQLException e) {
			if (metadataMgr != null) {
				metadataMgr.close();
			}
			tracer.error("SyncLite Logger failed during logger initialization for db : " + dbPath, e);
			throw new SQLException("SyncLite Logger failed during logger initialization for db : " + dbPath, e);
		}
	}

	private final void tryLockDB() throws SQLException {
		appLock.tryLock(this.dbPath);
	}

	protected abstract void initLogger();

	static SQLLogger findInstance(Path dbPath) throws SQLException {
		if (dbPath == null) {
			return null;
		}
		return (SQLLogger) loggers.get(dbPath);
	}

	protected abstract LogSegmentPlacer getLogSegmentPlacer();

	protected abstract void startLogging();

	protected abstract long getDefaultPageSize();

	final boolean isLoggerHealthy() {
		return isHealthy.get();
	}

	final long getCommitID() {
		return this.currentTxnCommitId;
	}

	final long getOperationID() {
		return this.currentOperationId;
	}

	final void incrCurrentOperationID() {
		++this.currentOperationId;
	}
	
	final long getCurrentLogSegmentSequenceNumber() {
		return logSegmentSequenceNumber.get();
	}
	
	private final void initializeLogShipper() throws SQLException {
		//If there is single destination then we only create a LogMover 
		//Else we create multiple LogShippers + LogCleaner
		//
		if ((options.getNumDestinations() == 1)) {
			LogMover mover = new LogMover(this.dbPath, this.databaseID, getWriteArchiveName(), logSegmentPlacer, metadataMgr, this.options, 1, this.tracer);
			logShippers.add(mover);
			mover.setLogSegmentSequenceNumber(this.logSegmentSequenceNumber);
			mover.setDataFileSequenceNumber(this.dataFileSequenceNumber);
		} else {
			for (Integer i=1 ; i <= options.getNumDestinations(); ++i) {
				LogShipper shipper = new LogShipper(this.dbPath, this.databaseID, getWriteArchiveName(), logSegmentPlacer, this.metadataMgr, this.options, i, this.tracer);
				logShippers.add(shipper);
				shipper.setLogSegmentSequenceNumber(this.logSegmentSequenceNumber);
				shipper.setDataFileSequenceNumber(this.dataFileSequenceNumber);
			}
			logCleaner = new LogCleaner(this.dbPath, this.databaseID, this.logShippers, this.logSegmentPlacer, metadataMgr, this.options, this.tracer);			
		}
	}

	private final void initializeCommandHandlers() throws SQLException {
		//
		//If command handler is enabled then start command handler
		//
		if (options.getEnableCommandHandler()) {
			for (Integer i=1 ; i <= options.getNumDestinations(); ++i) {
				CommandHandler cmdHandler = new CommandHandler(this.dbPath, getReadArchiveName(), this.metadataMgr, this.options, i, this.tracer);
				cmdHandlers.add(cmdHandler);
			}
		}
	}

	private final void initializeBackupAgent() throws SQLException {
		if (this.backupShipped == 0) {
			if (options.getNumDestinations() > 1) {
				this.backupAgent = new BackupAgentMultiDest(this.dbPath, getWriteArchiveName(), getDataBackupSuffix(), metadataMgr, options, tracer);
			} else {
				this.backupAgent = new BackupAgent(this.dbPath, getWriteArchiveName(), getDataBackupSuffix(), metadataMgr, options, tracer);
			}
		}
	}

	private String getWriteArchiveName() {
		if (options.getDeviceName().equals("")) {
			return getWriteArchiveNamePrefix() + this.uuid.toString();
		}
		return getWriteArchiveNamePrefix() + options.getDeviceName() + "-" + this.uuid.toString();
	}

	private String getReadArchiveName() {
		if (options.getDeviceName().equals("")) {
			return getReadArchiveNamePrefix() + this.uuid.toString();
		}
		return getReadArchiveNamePrefix() + options.getDeviceName() + "-" + this.uuid.toString();
	}

	private final void doSwitchLogSegment() throws SQLException {
		finishCurrentLogSegment();
		createNewLogSegment(this.logSegmentSequenceNumber.get() + 1);
		this.logSegmentSequenceNumber.addAndGet(1);
		this.logSegmentLogCount = 0;

		metadataMgr.updateProperty("log_segment_sequence_number", String.valueOf(logSegmentSequenceNumber));
		for (LogShipper logShipper : logShippers)
		{
			logShipper.setLogSegmentSequenceNumber(this.logSegmentSequenceNumber);
		}
	}

	protected final void finishCurrentLogSegment() throws SQLException {
		if (logTableConn != null) {
			try {
				try (Statement stmt = logTableConn.createStatement()) {
					stmt.execute(updateMetadataTableSql);
				}
				logTableConn.commit();

				if (additionalPrepStmts != null) {
					for (PreparedStatement pstmt : additionalPrepStmts) {
						pstmt.close();
					}
					additionalPrepStmts.clear();
					argTablePrepStmtsMap.clear();
				}				
				if (insertLogTablePstmt != null) {
					insertLogTablePstmt.close();
				}

				logTableConn.close();
				logTableConn = null;
			} catch (SQLException e) {
				//suppress
			}
		}
	}

	protected final void closeCurrentLogSegment() throws SQLException {
		if (logTableConn != null) {
			try {				
				if (additionalPrepStmts != null) {
					for (PreparedStatement pstmt : additionalPrepStmts) {
						pstmt.close();
					}
					additionalPrepStmts.clear();
					argTablePrepStmtsMap.clear();
				}				
				if (insertLogTablePstmt != null) {
					insertLogTablePstmt.close();
				}
				logTableConn.close();
			} catch (SQLException e) {
				tracer.error("SyncLite failed to close log table connection for current log segment : " + e);
			}    			
		}
	}

	private final void initializeMetadataProperties() throws SQLException {
		this.metadataMgr = new MetadataManager(getMetadataFilePath(dbPath));
		String strVal = metadataMgr.getStringProperty("uuid");
		if (strVal != null) {
			this.uuid = UUID.fromString(strVal);
		} else {
			if (options.getUUID() != null) {
				this.uuid = UUID.fromString(options.getUUID());
			} else {
				this.uuid = UUID.randomUUID();
			}
			metadataMgr.insertProperty("uuid", this.uuid.toString());
		}
		options.setUUID(this.uuid.toString());

		strVal = metadataMgr.getStringProperty("device_name");
		if (strVal != null) {
			this.deviceName = strVal;
		} else {
			this.deviceName = options.getDeviceName();
			metadataMgr.insertProperty("device_name", this.deviceName);
		}

		strVal = metadataMgr.getStringProperty("database_name");
		if (strVal == null) {
			metadataMgr.insertProperty("database_name", this.dbName.toString());
		}

		Long longVal = metadataMgr.getLongProperty("backup_shipped");
		if (longVal != null) {
			this.backupShipped = longVal;
		} else {
			this.backupShipped = 0;
			metadataMgr.insertProperty("backup_shipped", this.backupShipped);
		}

		longVal = metadataMgr.getLongProperty("log_segment_sequence_number");
		if (longVal != null) {
			this.logSegmentSequenceNumber.set(longVal);
		} else {
			this.logSegmentSequenceNumber.set(-1);
			metadataMgr.insertProperty("log_segment_sequence_number", this.logSegmentSequenceNumber);
		}

		longVal = metadataMgr.getLongProperty("data_file_sequence_number");
		if (longVal != null) {
			this.dataFileSequenceNumber.set(longVal);
		} else {
			this.dataFileSequenceNumber.set(-1);
			metadataMgr.insertProperty("data_file_sequence_number", this.dataFileSequenceNumber);
		}

		longVal = metadataMgr.getLongProperty("database_id");
		if (longVal != null) {
			this.databaseID = longVal;
		} else {
			if (options.getDatabaseId() != -1) {
				this.databaseID = options.getDatabaseId();
			} else {
				this.databaseID = 0;
			}
			metadataMgr.insertProperty("database_id", this.databaseID);
		}
		options.setDatabaseId(this.databaseID);

		longVal = metadataMgr.getLongProperty("last_processed_request_id");
		if (longVal != null) {
			this.lastProcessedRequestID = longVal;
		} else {
			this.lastProcessedRequestID = 0;
			metadataMgr.insertProperty("last_processed_request_id", this.lastProcessedRequestID);
		}

		longVal = metadataMgr.getLongProperty("last_processing_request_id");
		if (longVal != null) {
			this.lastProcessingRequestID=longVal;
		} else {
			this.lastProcessingRequestID=0;
			metadataMgr.insertProperty("last_processing_request_id", this.lastProcessingRequestID);
		}

		strVal = metadataMgr.getStringProperty("last_processing_command");
		if (strVal != null) {
			this.lastProcessingCommand = strVal;
		} else {
			this.lastProcessingCommand = "";
			metadataMgr.insertProperty("last_processing_command", this.lastProcessingCommand);
		}

		initializeAdditionalMetadataProperties();
	}

	protected abstract void initializeAdditionalMetadataProperties() throws SQLException;


	private final void restartRecovery() throws SQLException {

		DBProcessor dbReader = DBProcessor.getInstance(options.getDeviceType());
		HashMap<String, Long> result = dbReader.initReadCommitID(dbPath, getDefaultPageSize());
		this.restartMasterCommitID = result.get("commit_id");
		this.currentOperationId = result.get("operation_id");		
		this.latestGeneratedCommitId.set(restartMasterCommitID);
		
		if (this.logSegmentSequenceNumber.get() == -1) {
			doSwitchLogSegment();
		} else {
			try {
				//In case the log segment was not initialized properly, while the application stopped
				//try to do this again on startup
				//
				initLogSegment(this.logSegmentSequenceNumber.get());
				reloadCurrentLogSegment();
			} catch (SQLException e) {
				//If restart recovery is disabled, create a fresh segment and start up
				if (options.getSkipRestartRecovery()) {
					this.logSegmentSequenceNumber.decrementAndGet();
					doSwitchLogSegment();
				} else {
					tracer.error("Restart recovery for device : " + dbPath + " failed with exception : ", e);
					throw e;
				}
			}
		}
		this.currentBatchLogCount = 0;
		this.currentTxnCommitId = this.restartMasterCommitID;
		this.lastLogSegmentCreateTime = System.currentTimeMillis();
		try {
			resolveInDoubtTxn();
		} catch (SQLException e) {
			if (! options.getSkipRestartRecovery()) {
				tracer.error("Restart recovery for device : " + dbPath + " failed with exception : ", e);
				throw new SQLException ("Restart recovery exception : ", e);
			}
		}		
		
		if (this.allowsConcurrentWrites) {
			SQLStager.removeOrphanCmdFiles(dbPath);
		}
	}

   long getNextCommitID() {
        return latestGeneratedCommitId.updateAndGet(current -> {
            long newCommitID = System.currentTimeMillis();
            return newCommitID > current ? newCommitID : current + 1;
        });
   }


	protected abstract void undoLogsForCommit(long commitId) throws SQLException;
	protected abstract void resolveInDoubtTxn() throws SQLException;

	protected abstract String getLogSegmentSignature();

	protected abstract String getMetadtaFileSuffix();

	protected abstract Path getMetadataFilePath(Path dbPath);

	protected abstract String getWriteArchiveNamePrefix();
	protected abstract String getReadArchiveNamePrefix();

	protected abstract String getDataBackupSuffix();

	private final void createNewLogSegment(long seqNum) throws SQLException {
		initLogSegment(seqNum);
	}

	private final void initLogSegment(long seqNum) throws SQLException {
		this.logPath = logSegmentPlacer.getLogSegmentPath(this.dbPath, this.databaseID, seqNum);
		String url = "jdbc:sqlite:" + logPath;
		logTableConn = DriverManager.getConnection(url);
		inlinedArgCnt = options.getLogMaxInlinedArgs();
		String argList = SyncLiteUtils.prepareArgList(inlinedArgCnt);
		String fillerList = SyncLiteUtils.preparePStmtFillerList(inlinedArgCnt + 4);
		try (Statement stmt = logTableConn.createStatement()) {
			stmt.execute("pragma journal_mode = normal;");
			stmt.execute("pragma synchronous = normal;");
			stmt.execute("pragma temp_store = memory;");
			stmt.execute("pragma mmap_size = 30000000000;");
			stmt.execute("pragma page_size = " + options.getLogSegmentPageSize()+ ";");
			stmt.execute(createLogTableSqlTemplate.replace("$1", argList));
			stmt.execute(dropMetadataTableSql);
			stmt.execute(createMetadataTableSql);
			stmt.execute(insertMetadataTableSql);
		}
		logTableConn.setAutoCommit(false);
		String insertLogTableSql = insertLogTableSqlTemplate.replace("$1", argList);
		insertLogTableSql = insertLogTableSql.replace("$2", fillerList);
		insertLogTablePstmt = logTableConn.prepareStatement(insertLogTableSql);
		lastLogSegmentCreateTime = System.currentTimeMillis();		
	}

	private final void reloadCurrentLogSegment() throws SQLException {
		this.logPath = logSegmentPlacer.getLogSegmentPath(this.dbPath, this.databaseID, this.logSegmentSequenceNumber.get());
		String url = "jdbc:sqlite:" + logPath;
		logTableConn = DriverManager.getConnection(url);
		logTableConn.setAutoCommit(false);
		restartSlaveCommitID = 0;
		currentTxnLogCount = 0;
		restartTxnFate = "UNKNOWN";
		restartLoggedSQL = null;
		try (Statement stmt = logTableConn.createStatement()) {
			try (ResultSet rs = stmt.executeQuery("SELECT commit_id FROM commandlog WHERE change_number = (SELECT MAX(change_number) FROM commandlog);")) {
				if (rs.next()) {
					restartSlaveCommitID = rs.getLong(1);
				}
			}

			try (ResultSet rs = stmt.executeQuery("SELECT sql FROM commandlog WHERE change_number = (SELECT MAX(change_number) FROM commandlog)")) {
				if (rs.next()) {
					restartLoggedSQL = rs.getString(1);
				}
			}

			if (restartLoggedSQL != null) {
				if (restartLoggedSQL.equalsIgnoreCase("COMMIT")) {
					restartTxnFate = "COMMIT";
				} else if (restartLoggedSQL.equalsIgnoreCase("ROLLBACK")) {
					restartTxnFate = "ROLLBACK";
				} else {
					/*try (ResultSet rs = stmt.executeQuery("SELECT sql FROM commandlog WHERE change_number = (SELECT MAX(change_number) FROM commandlog) AND (sql = 'COMMIT' OR sql = 'ROLLBACK')")) {
						if (rs.next()) {
							restartTxnFate = rs.getString(1);
						}
					} */           		
				}
			}

			try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM commandlog;")) {
				if (rs.next()) {
					this.logSegmentLogCount = rs.getLong(1);
				}
			}
			
			try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM commandlog WHERE commit_id = " + restartSlaveCommitID)) {
				if (rs.next()) {
					this.currentTxnLogCount = rs.getLong(1);
				}
			}
			
			long colCnt = 0;
			try (ResultSet rs = stmt.executeQuery("pragma table_info(commandlog)")) {
				while (rs.next()) {
					++colCnt;
				}
			}
			inlinedArgCnt = colCnt - 4;
		} catch (SQLException e) {
			this.logTableConn = null;
			tracer.error("SyncLite Logger failed reloading log segment " + logPath + " with exception : ", e);
			throw new SQLException("SyncLite Logger failed reloading log segment " + logPath + " with exception : ", e);
		}
		String argList = SyncLiteUtils.prepareArgList(inlinedArgCnt);
		String fillerList = SyncLiteUtils.preparePStmtFillerList(inlinedArgCnt + 4);
		String insertLogTableSql = insertLogTableSqlTemplate.replace("$1", argList);
		insertLogTableSql = insertLogTableSql.replace("$2", fillerList);
		insertLogTablePstmt = logTableConn.prepareStatement(insertLogTableSql);
	}

	abstract void log(long commitId, String sql, Object[] args) throws SQLException;

	abstract void flush(long commitId) throws SQLException;    


	protected final void clearLogBatch() throws SQLException {
		insertLogTablePstmt.clearBatch();
		for (PreparedStatement pstmt : additionalPrepStmts) {
			pstmt.clearBatch();
		}
		additionalPrepStmts.clear();
	}

	protected final void executeLogBatch() throws SQLException {
		insertLogTablePstmt.executeBatch();
		for (PreparedStatement pstmt : additionalPrepStmts) {
			pstmt.executeBatch();
		}		
		currentBatchLogCount = 0;
	}

	private final void rePrepareLogTablePstmt() throws SQLException {
		String argList = SyncLiteUtils.prepareArgList(inlinedArgCnt);
		String fillerList = SyncLiteUtils.preparePStmtFillerList(inlinedArgCnt + 4);
		String insertLogTableSql = insertLogTableSqlTemplate.replace("$1", argList);
		insertLogTableSql = insertLogTableSql.replace("$2", fillerList);
		insertLogTablePstmt = logTableConn.prepareStatement(insertLogTableSql);		
	}

	private final void addNewInlinedArgCols(long startIndex, long endIndex) throws SQLException {
		for (long i=startIndex; i <=endIndex; ++i) {
			try (Statement stmt = logTableConn.createStatement()) {
				String sql = alterLogTableSqlTemplate.replace("$1", "arg" +i);
				stmt.execute(sql);
			}
		}
		this.inlinedArgCnt = endIndex;
	}

	protected final void appendLogRecord(CommandLogRecord record) throws SQLException {
		if ((record.args != null) && (record.args.length > inlinedArgCnt)) {
			//
			//Flush current batch if non empty
			//Add new arg columns to commandlog table
			//Reprepare log insert prepared statement.
			//
			if (currentBatchLogCount > 0) {
				executeLogBatch();
				clearLogBatch();
			}
			addNewInlinedArgCols(inlinedArgCnt + 1, record.args.length);
			rePrepareLogTablePstmt();
		}
		insertLogTablePstmt.clearParameters();
		insertLogTablePstmt.setObject(1, logSegmentLogCount);
		insertLogTablePstmt.setObject(2, record.commitId);
		insertLogTablePstmt.setObject(3, record.sql);
		if (record.args != null) {
			insertLogTablePstmt.setObject(4, record.args.length);
			for (int i = 1; i <= record.args.length; i++) {
				insertLogTablePstmt.setObject(i + 4, record.args[i - 1]);
			}
		} else {
			insertLogTablePstmt.setObject(4, 0);
		}
		insertLogTablePstmt.addBatch();
		this.currentTxnCommitId = record.commitId;
		++currentBatchLogCount;
		++currentTxnLogCount;
		++logSegmentLogCount;
		++currentOperationId;
	}

	/*
	 * OLD VERSION THAT USES extedned args table.
	 * 
	protected final void appendLogRecord(CommandLogRecord record) throws SQLException {
		insertLogTablePstmt.clearParameters();
		insertLogTablePstmt.setObject(1, logSegmentLogCount);
		insertLogTablePstmt.setObject(2, record.commitId);
		insertLogTablePstmt.setObject(3, record.sql);
		if (record.args != null) {
			insertLogTablePstmt.setObject(4, record.args.length);
			if (record.args.length <= options.getLogMaxInlinedArgs()) {
				for (int i = 1; i <= record.args.length; i++) {
					insertLogTablePstmt.setObject(i + 4, record.args[i - 1]);
				}
			} else {
				PreparedStatement argTablePstmt = getExtendedArgTablePrepStmt(record.args.length);
				try {
					argTablePstmt.setObject(1, logSegmentLogCount);
				} catch (SQLException e) {
					throw e;
				}
				for (int i = 1; i <= record.args.length; i++) {
					argTablePstmt.setObject(i+1, record.args[i - 1]);
				}
				argTablePstmt.addBatch();
				additionalPrepStmts.add(argTablePstmt);
			}
		} else {
			insertLogTablePstmt.setObject(4, 0);
		}
		insertLogTablePstmt.addBatch();
		this.currentTxnCommitId = record.commitId;
		++currentBatchLogCount;
		++currentTxnLogCount;
		++logSegmentLogCount;
		++currentOperationId;
	}

	 */

	private PreparedStatement getExtendedArgTablePrepStmt(long argCnt) throws SQLException {
		long argTableNum = SyncLiteUtils.nextPowerOf2(argCnt);
		PreparedStatement argTablePstmt = argTablePrepStmtsMap.get(argTableNum);
		if (argTablePstmt == null) {
			String createSql = generateCreateArgTableSql(argTableNum);
			try (Statement stmt = logTableConn.createStatement()) {
				stmt.execute(createSql);
				String insertSql = generateInsertArgTableSql(argTableNum);
				argTablePstmt = logTableConn.prepareStatement(insertSql);
				argTablePrepStmtsMap.put(argTableNum, argTablePstmt);
			}
		}
		return argTablePstmt;
	}

	private String generateInsertArgTableSql(long argTableNum) {
		StringBuilder colListBuilder = new StringBuilder();
		StringBuilder valListBuilder = new StringBuilder();
		colListBuilder.append("(change_number");
		valListBuilder.append("(?");
		for (int i = 1; i <=argTableNum; ++i) {
			colListBuilder.append(", ");
			colListBuilder.append("arg" + i);
			valListBuilder.append(", ");
			valListBuilder.append("?");
		}
		colListBuilder.append(")");
		valListBuilder.append(")");
		return "INSERT INTO arg" + argTableNum + colListBuilder.toString() + " VALUES " + valListBuilder.toString();
	}

	private final String generateCreateArgTableSql(long argTableNum) {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TABLE IF NOT EXISTS arg" + argTableNum + "(change_number");
		for (int i = 1; i <=argTableNum; ++i) {
			sqlBuilder.append(", ");
			sqlBuilder.append("arg" + i);
		}
		sqlBuilder.append(")");
		return sqlBuilder.toString();
	}

	protected final void commitLogSegment() throws SQLException {
		logTableConn.commit();
	}
	
	protected final void rollbackLogSegment() throws SQLException {
		logTableConn.rollback();
	}

	private final void checkAndSwitchLogSegment() throws SQLException {
		//Switch log segment only at a commit boundary
		if (currentTxnLogCount == 0) {
			//Switch log segment only if we have written something to current log segment
			if (this.logSegmentLogCount > 0) {
				long currentTime = System.currentTimeMillis();
				//Switch log segment only if
				//- current log segment count is greater than the specified log segment size threshold
				//- current log segment is older than the specified duration
				//- Logger is terminated as part of application shutdown.
				if ((this.logSegmentLogCount > options.getLogSegmentSwitchLogCountThreshold()) ||
						((currentTime - this.lastLogSegmentCreateTime) > options.getLogSegmentSwitchDurationThresholdMs()) ||
						terminateInProgress)
				{
					doSwitchLogSegment();
				}
			}
		}
	}

	protected void checkups() throws SQLException {  	
		checkAndSwitchLogSegment();
	}

	protected abstract void terminateInternal();

	private synchronized void terminate() throws SQLException {
		this.terminateInProgress = true;
		terminateInternal();
		if (backupAgent != null) {
			backupAgent.terminate();
		}
		for (LogShipper logShipper : logShippers) {
			logShipper.terminate();
		}
		if (logCleaner != null) {
			logCleaner.terminate(); 
		}
		for (CommandHandler cmdHandler : cmdHandlers) {
			cmdHandler.terminate();
		}
		if (metadataMgr != null) {
			metadataMgr.close();
		}

		if (tracer != null) {
			tracer.getAppender("SyncLiteLogger").close();		
		}
		appLock.release();
	}

	private final void cleanLogger() throws SQLException {
		//1.Delete all not-yet-copied command log segments
		try {
			Path parentPath = this.dbPath.getParent();
			String commandLogPrefix = this.dbName + getLogSegmentSignature();
			Files.walk(parentPath).filter(s -> (s.getFileName().toString().startsWith(commandLogPrefix) || s.getFileName().toString().endsWith(getMetadtaFileSuffix()))).map(Path::toFile).forEach(File::delete);
		} catch (IOException e) {
			//tracer.error("SyncLite Logger failed to cleanup older command log segments in db directory during resync request processing : ", e);
			throw new SQLException("SyncLite Logger failed to cleanup older command log segments in db directory during resync request processing : ", e);
		}

		//2.Delete the contents of local write archive
		for (Integer i = 1; i <= options.getNumDestinations(); ++i) {
			Path writeArchivePath = Path.of(options.getLocalDataStageDirectory(i).toString(), getWriteArchiveName() + "/");
			try {
				String commandLogPrefix = this.dbName + getLogSegmentSignature();
				Files.walk(writeArchivePath).filter(s -> (s.getFileName().toString().startsWith(commandLogPrefix) ||  s.getFileName().toString().endsWith(getDataBackupSuffix()) || s.getFileName().toString().endsWith(getMetadtaFileSuffix()))).map(Path::toFile).forEach(File::delete);
			} catch (IOException e) {
				//tracer.error("SyncLite Logger failed to cleanup contents of local data directory : " + writeArchivePath + " during resync request processing : ", e);
				throw new SQLException("SyncLite Logger failed to cleanup contents of local data directory : " + writeArchivePath + " during resync request processing : ", e);
			}
		}
	}

	private synchronized void closeDevice() throws SQLException {
		loggers.remove(this.dbPath);
		terminate();
	}

	static void closeAllDevices() throws SQLException {
		for(SQLLogger l : loggers.values()) {
			l.closeDevice();
		}		
		loggers.clear();
	}

	static void closeDevice(Path dbPath) throws SQLException {
		SQLLogger logger = (SQLLogger) SQLLogger.findInstance(dbPath);
		if (logger != null) {
			logger.closeDevice();
		}	
	}

	static final void cleanUpDevice(Path dbPath) throws SQLException {
		SQLLogger logger = (SQLLogger) SQLLogger.findInstance(dbPath);
		if (logger != null) {
			logger.closeDevice();
			logger.cleanLogger();
			logger.resetDeviceCheckpoint();
		} 	
	}

	private final void resetDeviceCheckpoint() throws SQLException {
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(dropTxnTableSql);
			}
		}
	}

	static final SyncLiteOptions getSyncLiteOptions(Path dbPath) throws SQLException {
		SQLLogger logger = (SQLLogger) SQLLogger.findInstance(dbPath);
		if (logger != null) {
			return logger.options;
		} else {
			throw new SQLException("SyncLite device : " + dbPath + " not initialized.");
		}		
	}

	final Path logDataFile(Path sourceFilePath) throws SQLException {
		try {
			long seqNum = dataFileSequenceNumber.get() + 1;
			Path dataFile = logSegmentPlacer.getDataFilePath(this.dbPath, this.databaseID, seqNum);
			//Copy the supplied sourceFilePath to dataFile		
			Files.copy(sourceFilePath, dataFile);
			dataFileSequenceNumber.addAndGet(1L);
			metadataMgr.updateProperty("data_file_sequence_number", String.valueOf(dataFileSequenceNumber.get()));			
			for (LogShipper logShipper : logShippers)
			{
				logShipper.setDataFileSequenceNumber(this.dataFileSequenceNumber);
			}
			return dataFile;
		} catch (Exception e) {
			throw new SQLException("SyncLite Logger failed to process data file : " + sourceFilePath, e);
		}
	}
}
