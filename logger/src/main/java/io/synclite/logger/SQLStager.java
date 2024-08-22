package io.synclite.logger;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

abstract class SQLStager {
	protected static final String createLogTableSqlTemplate = "CREATE TABLE IF NOT EXISTS commandlog(change_number INTEGER PRIMARY KEY, commit_id LONG, sql TEXT, arg_cnt INTEGER, $1);";
	protected static final String insertLogTableSqlTemplate = "INSERT INTO commandlog(change_number, commit_id, sql, arg_cnt, $1) VALUES ($2)";
	protected static final String alterLogTableSqlTemplate = "ALTER TABLE commandlog ADD COLUMN $1";

	private long txnID;
	protected Path dbPath;
	protected Path txnFilePath;
	protected LogSegmentPlacer logSegmentPlacer;
	private Connection logTableConn;
	private SyncLiteOptions options;
	private long inlinedArgCnt;
	protected PreparedStatement insertLogTablePstmt = null;
	private long currentBatchLogCount;
	protected long logSegmentLogCount;

	SQLStager(Path dbPath, SyncLiteOptions options, long txnID) throws SQLException {
		this.dbPath = dbPath;
		this.options = options;
		this.txnID = txnID;
		this.currentBatchLogCount = 0;
		this.logSegmentLogCount = 0;
		this.logSegmentPlacer = new TxnLoggerLogSegmentPlacer();
		initTxnStageFile();
	}
	
	String getFileName() {
		return this.txnFilePath.getFileName().toString();
	}
	
	private final void initTxnStageFile() throws SQLException {
		this.txnFilePath = logSegmentPlacer.getTxnStageFilePath(this.dbPath, this.txnID);
		String url = "jdbc:sqlite:" + txnFilePath;
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
		}
		logTableConn.setAutoCommit(false);
		String insertLogTableSql = insertLogTableSqlTemplate.replace("$1", argList);
		insertLogTableSql = insertLogTableSql.replace("$2", fillerList);
		insertLogTablePstmt = logTableConn.prepareStatement(insertLogTableSql);
	}	
	
	private final void rePrepareLogTablePstmt() throws SQLException {
		String argList = SyncLiteUtils.prepareArgList(inlinedArgCnt);
		String fillerList = SyncLiteUtils.preparePStmtFillerList(inlinedArgCnt + 4);
		String insertLogTableSql = insertLogTableSqlTemplate.replace("$1", argList);
		insertLogTableSql = insertLogTableSql.replace("$2", fillerList);
		insertLogTablePstmt = logTableConn.prepareStatement(insertLogTableSql);		
	}

	long getLogSegmentLogCount() {
		return logSegmentLogCount;
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

	protected final void executeLogBatch() throws SQLException {
		insertLogTablePstmt.executeBatch();
		insertLogTablePstmt.clearBatch();
		currentBatchLogCount = 0;
	}

	void log(long commitID, String sql, Object[] args) throws SQLException {
		if ((args != null) && (args.length > inlinedArgCnt)) {
			//
			//Flush current batch if non empty
			//Add new arg columns to commandlog table
			//Re-prepare log insert prepared statement.
			//
			if (currentBatchLogCount > 0) {
				executeLogBatch();
			}
			addNewInlinedArgCols(inlinedArgCnt + 1, args.length);
			rePrepareLogTablePstmt();
		}
		
		if (currentBatchLogCount > options.getLogSegmentFlushBatchSize()) {
			executeLogBatch();
			currentBatchLogCount = 0;
		}

		insertLogTablePstmt.clearParameters();
		insertLogTablePstmt.setObject(1, logSegmentLogCount);
		insertLogTablePstmt.setObject(2, 0);
		insertLogTablePstmt.setObject(3, sql);
		if (args != null) {
			insertLogTablePstmt.setObject(4, args.length);
			for (int i = 1; i <= args.length; i++) {
				insertLogTablePstmt.setObject(i + 4, args[i - 1]);
			}
		} else {
			insertLogTablePstmt.setObject(4, 0);
		}
		insertLogTablePstmt.addBatch();
		++currentBatchLogCount;
		++logSegmentLogCount;
	}
	
	
	void commit() throws SQLException {
		if (this.insertLogTablePstmt != null) {
			this.insertLogTablePstmt.executeBatch();
			this.insertLogTablePstmt.clearBatch();
		}
		if (this.logTableConn != null) {
			this.logTableConn.commit();
		}
	}

	public void close() throws SQLException{
		if (this.insertLogTablePstmt != null) {
			this.insertLogTablePstmt.close();
		}
		if (this.logTableConn != null) {
			this.logTableConn.close();
		}	
	}

	void cleanup() throws SQLException {
		close();
		try {
			Files.delete(this.txnFilePath);
		} catch (IOException e) {
			//Ignore for now.
		}
	}
	
	void rollback() throws SQLException {
		if (this.logTableConn != null) {
			this.logTableConn.rollback();
		}
		cleanup();
	}
	
	protected abstract void publishTxn(long logSeqNum, long commitID) throws SQLException;

	public static void removeOrphanCmdFiles(Path dbPath) {
		Path dbDir = Path.of(dbPath.toString() + ".synclite");
		
		String cmdFileSuffix = Telemetry.getSqlFileSignature();
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dbDir)) {
            for (Path entry : stream) {
            	if (entry.getFileName().toString().endsWith(cmdFileSuffix)) {
            		if (Files.exists(entry)) {
            			Files.delete(entry);
            		}
            	}
            }
        } catch (IOException e) {
        	//Ignore
        }		
	}
	
}
