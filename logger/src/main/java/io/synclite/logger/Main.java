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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {

	private static class DBThread extends Thread {
		String url;
		String tabName;

		public DBThread(String url, String tabName) {
			this.url = url;
			this.tabName = tabName;
		}

		public void run() {
			try (Connection conn = DriverManager.getConnection(url)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists " + tabName + "(a int, b int)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tabName + " VALUES(?,?)")) {

					for (int i = 1; i <= 100; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();						
					}
					pstmt.executeBatch();
				}			

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE " + tabName + " set b = ? WHERE a < ?")) {
					pstmt.setInt(1, 1000);
					pstmt.setInt(2, 10);
					pstmt.addBatch();
					pstmt.executeBatch();
				}		

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tabName + " WHERE a > ?")) {
					pstmt.setInt(1, 90);
					pstmt.execute();
				}		
				conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static Path syncLiteHome;
	public static Path syncLiteDB;
	public static Path syncLiteStage;
	public static Path syncLiteLoggerConfig;
	public static void main(String[] args) throws Exception	{
		{			
			setupHome();
			Class.forName("io.synclite.logger.SQLite");
			Class.forName("io.synclite.logger.Telemetry");
			Class.forName("io.synclite.logger.SQLiteAppender");
			Class.forName("io.synclite.logger.DuckDB");
			Class.forName("io.synclite.logger.Derby");

			//testDemoDBTran();
			//testSQLite();
			testSQLiteAppender();
			//testDuckDB();
			//testDuckDBAppender();
			//testH2();
			//testH2Appender();
			//testHyperSQL();
			//testStreaming();
			//testDerby();
			//testTelemetry();
			//testAppender();
			//testTelemetryInsertColList();
			//testKafkaProducer();

			{
				SQLite.closeAllDevices();
			}
		}
	}

	private static void testKafkaProducer() throws Exception {

		Properties props = new Properties();

		Producer<String, String> producer = new io.synclite.logger.KafkaProducer(props);

		// Send messages
		for (int i = 0; i < 1000000; i++) {
			String key = "key-" + i;
			String value = "value-" + i;
			ProducerRecord<String, String> record = new ProducerRecord<>("test", key, value);

			producer.send(record);
			// Send the record
			/*
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.printf("Sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d)\n",
                                key, value, metadata.partition(), metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
			 */
		}

		producer.close();
	}

	private static void setupHome() throws IOException {
		Path userHome = Path.of(System.getProperty("user.home"));
		syncLiteHome = userHome.resolve("synclite");
		syncLiteDB = syncLiteHome.resolve("job1").resolve("db");
		syncLiteStage = syncLiteHome.resolve("job1").resolve("stageDir");
		syncLiteLoggerConfig = syncLiteDB.resolve("synclite_logger.conf"); 
		Files.createDirectories(syncLiteDB);
		Files.createDirectories(syncLiteStage);
		if (!Files.exists(syncLiteLoggerConfig)) {
			StringBuilder sb = new StringBuilder();
			sb.append("local-data-stage-directory = ").append(syncLiteStage).append("\n");
			sb.append("destination-type = ").append("FS").append("\n");
			Files.writeString(syncLiteLoggerConfig, sb.toString());
		}
	}

	private static void testDemoDBTran() throws ClassNotFoundException, SQLException {
		Class.forName("io.synclite.logger.SQLite");
		Path dbPath = syncLiteDB.resolve("testDemoDBTran.db");
		Path configPath = syncLiteLoggerConfig;
		SQLite.initialize(dbPath, configPath, "testDemoDBTran");

		String url = "jdbc:synclite_sqlite:" + dbPath;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("create table if not exists t1(a int, b int)");
			}
		}
	}

	private static void testTelemetry() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.Telemetry");
		Path dbPath = syncLiteDB.resolve("testTelemetry.db");
		String url = "jdbc:synclite_telemetry:" + dbPath;
		{
			SyncLiteOptions options = new SyncLiteOptions();
			//options.setDestinationType(1, DestinationType.FS);
			//options.setLocalStageDirectory(1, Path.of("E:\\database\\stageDir"));
			//options.setDestinationType(2, DestinationType.FS);
			//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

			options.setDestinationType(1, DestinationType.FS);
			options.setLocalDataStageDirectory(1, syncLiteStage);
			options.setDeviceName("testTelemetry");
			Telemetry.initialize(dbPath, options);

			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
					stmt.execute("refresh table t1(a int, b int)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("insert into t1 values(?,?)")) {
					pstmt.setInt(1, 1);
					pstmt.setInt(2, 1);
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setInt(2, 2);
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}
		}
		{

			try (Connection conn = DriverManager.getConnection(url)) {
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {
					for (int i = 0 ; i < 1000010; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}			 
			}
		}

		/*
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("COPY t1 from 'c:\\synclite\\database\\t1.csv';COPY t1 from 'c:\\\\synclite\\\\database\\\\t1.csv' ");
			}
		}*/
	}


	private static void testTelemetryInsertColList() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.Telemetry");
		Path dbPath = syncLiteDB.resolve("testTelemetryInsertColList.db");
		String url = "jdbc:synclite_telemetry:" + dbPath;
		{
			SyncLiteOptions options = new SyncLiteOptions();
			//options.setDestinationType(1, DestinationType.FS);
			//options.setLocalStageDirectory(1, Path.of("E:\\database\\stageDir"));
			//options.setDestinationType(2, DestinationType.FS);
			//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

			options.setDestinationType(1, DestinationType.FS);
			options.setLocalDataStageDirectory(1, syncLiteStage);
			options.setDeviceName("testTelemetryInsertColList");
			Telemetry.initialize(dbPath, options);

			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("insert into t1(b, a) values(?,?)")) {
					pstmt.setInt(1, 10);
					pstmt.setInt(2, 1);
					pstmt.addBatch();

					pstmt.setInt(1, 20);
					pstmt.setInt(2, 2);
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}
		}
	}

	private static void testAppender() throws ClassNotFoundException, SQLException {
		testAppenderBasic();
		//testAppenderFatTable();
		//testAppenderAutoincrement();
		//testAppenderFatTableInlinedArgs();
		//testAppenderFatTableAutoInlinedArgs();
	}


	private static void testAppenderFatTable() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.Appender");

		Path dbPath = syncLiteDB.resolve("testAppenderFatTable.db");
		SyncLiteOptions options = new SyncLiteOptions();
		options.setDestinationType(1, DestinationType.FS);
		options.setLocalDataStageDirectory(1, syncLiteStage);
		options.setDeviceName("testAppenderFatTable");

		options.setLogSegmentFlushBatchSize(10);

		SQLiteAppender.initialize(dbPath, options);
		String url = "jdbc:synclite_appender:" + dbPath;
		long startT = System.currentTimeMillis();
		{
			int numCols = 100;
			int numRecs = 1000000;
			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				StringBuilder sqlBuilder = new StringBuilder();
				sqlBuilder.append("create table if not exists t1(");
				boolean first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						sqlBuilder.append("col" + i + " INTEGER PRIMARY KEY AUTOINCREMENT");
						//sqlBuilder.append("col" + i);
					} else {
						sqlBuilder.append(", col" + i);
					}

					first = false;
				}
				sqlBuilder.append(")");

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(sqlBuilder.toString());
				}


				StringBuilder insertBuilder = new StringBuilder();
				insertBuilder.append("INSERT INTO t1 VALUES(");
				first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						insertBuilder.append("NULL/*AUTOINCREMENT*/");
						//insertBuilder.append("?");
					} else {
						insertBuilder.append(",?");
					}					
					first = false;
				}
				insertBuilder.append(")");

				try (PreparedStatement pstmt = conn.prepareStatement(insertBuilder.toString())) {
					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j < numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();					
					}
					pstmt.executeBatch();

					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j < numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();

				}
			}
		}
		long finishT = System.currentTimeMillis();
		System.out.println("Time to execute fat table workload : " + (finishT - startT) / 1000 + " secs");

	}

	private static void testAppenderBasic() throws SQLException, ClassNotFoundException {
		{
			Class.forName("io.synclite.logger.Appender");
			Path dbPath = syncLiteDB.resolve("testAppenderBasic.db");
			Path configPath = syncLiteLoggerConfig;

			SQLiteAppender.initialize(dbPath, configPath, "testAppenderBasic");

			String url = "jdbc:synclite_appender:" + dbPath;
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("insert into t1 values(?,?)")) {
					pstmt.setInt(1, 1);
					pstmt.setInt(2, 1);
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setInt(2, 2);
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}
		}

		{

			Path dbPath = Path.of("C:\\synclite\\database\\db\\testAp1.db");
			String url = "jdbc:synclite_appender:" + dbPath;

			try (Connection conn = DriverManager.getConnection(url)) {
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {
					for (int i = 0 ; i < 1000000; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}			 
			}
		}
	}

	private static void testAppenderAutoIncrement() throws SQLException, ClassNotFoundException {
		{
			Class.forName("io.synclite.logger.Appender");
			Path dbPath = syncLiteDB.resolve("testAppenderAutoIncrement.db"); 
			Path config = syncLiteLoggerConfig;
			SQLiteAppender.initialize(dbPath, config, "testAppenderAutoIncrement");
			//SyncLiteTelemetry.initialize(dbPath, Path.of("E:\\database\\db\\synclite.props"));

			String url = "jdbc:synclite_appender:" + dbPath;
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a INTEGER PRIMARY KEY AUTOINCREMENT, b text)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("insert into t1 values(NULL/*AUTOINCREMENT*/,?)")) {
					pstmt.setString(1, "a");
					pstmt.addBatch();

					pstmt.setString(1, "b");
					pstmt.addBatch();

					pstmt.executeBatch();
				}

			}
		}

	}

	private static void testSQLite() throws ClassNotFoundException, SQLException {
		testSQLiteBasic();
		//testSQLiteFatTable();
		//testSQLiteFatTableInlinedArgs();
		//testSQLiteLoadExisting();
		//testBlob();
	}

	private static void testSQLiteAppender() throws ClassNotFoundException, SQLException {
		testSQLiteAppenderBasic();
	}
	private static void testBlob() throws ClassNotFoundException, SQLException {
		Path dbPath = syncLiteDB.resolve("testBlob.db");
		Path config = syncLiteLoggerConfig;

		SQLite.initialize(dbPath, config, "testBlob");

		String url = "jdbc:synclite_sqlite:" + dbPath;
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("create table if not exists t1(a int, b BLOB)");
			}

			try (PreparedStatement pstmt = conn.prepareStatement("insert into t1 values(?, ?)")) {
				pstmt.setInt(1, 1);
				byte[] arr = {1, 2, 3, 4};
				pstmt.setBytes(2, arr);	
				pstmt.addBatch();				
				pstmt.executeBatch();				

				try (Statement stmt = conn.createStatement()) {					
					try (ResultSet rs = stmt.executeQuery("select a,b from t1")) {
						if (rs.next()) {
							int a = rs.getInt("a");
							byte[] arr1 = rs.getBytes("b");						
							System.out.println("Read values : " + a + ", " + Arrays.toString(arr1));
						}
					}
				}
			}
		}		
	}

	private static void testSQLiteFatTable() throws ClassNotFoundException, SQLException {
		Class.forName("io.synclite.logger.SQLite");

		Path dbPath = syncLiteDB.resolve("testSQLiteFatTable.db");
		Path config = syncLiteLoggerConfig;		
		SQLite.initialize(dbPath, config, "testSQLiteFatTable");

		String url = "jdbc:synclite_sqlite:" + dbPath;

		long startT = System.currentTimeMillis();
		{
			int numCols = 100;
			int numRecs = 1000000;
			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				StringBuilder sqlBuilder = new StringBuilder();
				sqlBuilder.append("create table if not exists t1(");
				boolean first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						sqlBuilder.append("col" + i);
					} else {
						sqlBuilder.append(", col" + i);
					}

					first = false;
				}
				sqlBuilder.append(")");

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(sqlBuilder.toString());
				}


				StringBuilder insertBuilder = new StringBuilder();
				insertBuilder.append("INSERT INTO t1 VALUES(");
				first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						insertBuilder.append("?");
					} else {
						insertBuilder.append(",?");
					}					
					first = false;
				}
				insertBuilder.append(")");

				try (PreparedStatement pstmt = conn.prepareStatement(insertBuilder.toString())) {
					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j <= numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();


					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j <= numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();

				}		

			}
		}
		long finishT = System.currentTimeMillis();
		System.out.println("Time to execute fat table workload : " + (finishT - startT) / 1000 + " secs");

	}


	private static void testSQLiteFatTableInlinedArgs() throws ClassNotFoundException, SQLException {
		Class.forName("io.synclite.logger.SQLite");

		Path dbPath = syncLiteDB.resolve("testSQLiteFatTableInlinedArgs.db");
		Path config = syncLiteLoggerConfig;	
		SQLite.initialize(dbPath, config, "testSQLiteFatTableInlinedArgs");

		String url = "jdbc:synclite_sqlite:" + dbPath;

		SyncLiteOptions options = new SyncLiteOptions();
		options.setLogMaxInlineArgs(100);

		SQLite.initialize(dbPath, options);
		long startT = System.currentTimeMillis();
		{
			int numCols = 100;
			int numRecs = 10;
			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				StringBuilder sqlBuilder = new StringBuilder();
				sqlBuilder.append("create table if not exists t1(");
				boolean first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						sqlBuilder.append("col" + i);
					} else {
						sqlBuilder.append(", col" + i);
					}

					first = false;
				}
				sqlBuilder.append(")");

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(sqlBuilder.toString());
				}


				StringBuilder insertBuilder = new StringBuilder();
				insertBuilder.append("INSERT INTO t1 VALUES(");
				first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						insertBuilder.append("?");
					} else {
						insertBuilder.append(",?");
					}					
					first = false;
				}
				insertBuilder.append(")");

				try (PreparedStatement pstmt = conn.prepareStatement(insertBuilder.toString())) {
					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j <= numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();


					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j <= numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();

				}		

			}
		}
		long finishT = System.currentTimeMillis();
		System.out.println("Time to execute fat table workload : " + (finishT - startT) / 1000 + " secs");

	}

	private static void testAppenderFatTableInlinedArgs() throws ClassNotFoundException, SQLException {
		Class.forName("io.synclite.logger.Appender");

		Path dbPath = syncLiteDB.resolve("testAppenderFatTableInlinedArgs.db");
		Path config = syncLiteLoggerConfig;	
		SQLiteAppender.initialize(dbPath, config, "testAppenderFatTableInlinedArgs");

		String url = "jdbc:synclite_appender:" + dbPath;

		SyncLiteOptions options = new SyncLiteOptions();
		options.setLogMaxInlineArgs(100);

		SQLiteAppender.initialize(dbPath, options);
		long startT = System.currentTimeMillis();
		{
			int numCols = 100;
			int numRecs = 10;
			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				StringBuilder sqlBuilder = new StringBuilder();
				sqlBuilder.append("create table if not exists t1(");
				boolean first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						//sqlBuilder.append("col" + i);
						sqlBuilder.append("col" + i + " INTEGER PRIMARY KEY AUTOINCREMENT");
					} else {
						sqlBuilder.append(", col" + i);
					}

					first = false;
				}
				sqlBuilder.append(")");

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(sqlBuilder.toString());
				}


				StringBuilder insertBuilder = new StringBuilder();
				insertBuilder.append("INSERT INTO t1 VALUES(");
				first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						//insertBuilder.append("?");
						insertBuilder.append("NULL/*AUTOINCREMENT*/");
					} else {
						insertBuilder.append(",?");
					}					
					first = false;
				}
				insertBuilder.append(")");

				try (PreparedStatement pstmt = conn.prepareStatement(insertBuilder.toString())) {
					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j <= numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();


					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j <= numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();
				}				
			}
		}
		long finishT = System.currentTimeMillis();
		System.out.println("Time to execute fat table workload : " + (finishT - startT) / 1000 + " secs");

	}


	private static void testAppenderFatTableAutoInlinedArgs() throws ClassNotFoundException, SQLException {
		Class.forName("io.synclite.logger.Appender");

		Path dbPath = syncLiteDB.resolve("testAppenderFatTableAutoInlinedArgs.db");
		Path config = syncLiteLoggerConfig;	
		SQLiteAppender.initialize(dbPath, config, "testAppenderFatTableAutoInlinedArgs");

		String url = "jdbc:synclite_appender:" + dbPath;

		SyncLiteOptions options = new SyncLiteOptions();
		options.setLogMaxInlineArgs(4);

		SQLiteAppender.initialize(dbPath, options);
		long startT = System.currentTimeMillis();
		{
			int numCols = 100;
			int numRecs = 10;
			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				StringBuilder sqlBuilder = new StringBuilder();
				sqlBuilder.append("create table if not exists t1(");
				boolean first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						//sqlBuilder.append("col" + i);
						sqlBuilder.append("col" + i + " INTEGER PRIMARY KEY AUTOINCREMENT");
					} else {
						sqlBuilder.append(", col" + i);
					}

					first = false;
				}
				sqlBuilder.append(")");

				try (Statement stmt = conn.createStatement()) {
					stmt.execute(sqlBuilder.toString());
				}


				StringBuilder insertBuilder = new StringBuilder();
				insertBuilder.append("INSERT INTO t1 VALUES(");
				first = true;
				for (int i = 1; i <= numCols; ++i) {
					if (first == true) {
						//insertBuilder.append("?");
						insertBuilder.append("NULL/*AUTOINCREMENT*/");
					} else {
						insertBuilder.append(",?");
					}					
					first = false;
				}
				insertBuilder.append(")");

				try (PreparedStatement pstmt = conn.prepareStatement(insertBuilder.toString())) {
					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j <= numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();


					for (int i = 1; i <= numRecs; ++i) {
						for (int j = 1; j <= numCols; ++j) {
							pstmt.setInt(j, j);
						}
						pstmt.addBatch();						
					}
					pstmt.executeBatch();

				}		

			}
		}
		long finishT = System.currentTimeMillis();
		System.out.println("Time to execute fat table workload : " + (finishT - startT) / 1000 + " secs");

	}

	private static void testSQLiteBasic() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.SQLite");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testSQLiteBasic.db");
		Path config = syncLiteLoggerConfig;	
		SQLite.initialize(dbPath, config, "testSQLiteBasic");

		//SQLite.initialize(dbPath);
		String url = "jdbc:synclite_sqlite:" + dbPath;

		{

			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
					((SyncLiteStatement) stmt).executeUnlogged("insert into t1 values(0,0)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {

					for (int i = 1; i <= 1000010; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();						
					}
					pstmt.executeBatch();
				}			

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE t1 set b = ? WHERE a < ?")) {
					pstmt.setInt(1, 100);
					pstmt.setInt(1, 100);
					pstmt.addBatch();
					pstmt.executeBatch();
				}		

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM t1 WHERE a > ?")) {
					pstmt.setInt(1, 1000000);
					pstmt.addBatch();
					pstmt.executeBatch();
				}		
				//conn.commit();
			}
		}
	}

	private static void testSQLiteAppenderBasic() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.SQLiteAppender");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testSQLiteAppenderBasic.db");
		Path config = syncLiteLoggerConfig;	
		SQLiteAppender.initialize(dbPath, config, "testSQLiteAppenderBasic");

		String url = "jdbc:synclite_sqlite_appender:" + dbPath;

		{

			try (Connection conn = DriverManager.getConnection(url)) {
				conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
					//((SyncLiteAppenderStatement) stmt).executeUnlogged("insert into t1 values(0,0)");
					
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {

					for (int i = 1; i <= 100; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();						
					}
					pstmt.executeBatch();
				}			
				conn.commit();
				
			}
		}
	}


	private static void testSQLiteLoadExisting() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.SQLite");

		Path dbPath = syncLiteDB.resolve("testSQLiteLoadExisting.db");
		Path config = syncLiteLoggerConfig;	
		SQLite.initialize(dbPath, config, "testSQLiteLoadExisting");

		//SQLite.initialize(dbPath);
		String url = "jdbc:synclite_sqlite:" + dbPath;
		{
			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");					
				}
			}
		}
	}


	private static void testDuckDBBasic() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.DuckDB");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testDuckDBBasic.db");
		Path config = syncLiteLoggerConfig;	
		DuckDB.initialize(dbPath, config, "testDuckDBBasic");

		String url = "jdbc:synclite_duckdb:" + dbPath;

		{

			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
					stmt.executeUpdate("insert into t1 values(0,0)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("select a, b from t1")) {
					pstmt.executeQuery();
				}
				
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1(a, b) VALUES(?,?)")) {
					for (int i = 1; i <= 100; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}			

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE t1 set b = ? WHERE a < ?")) {
					pstmt.setInt(1, 1000);
					pstmt.setInt(2, 10);
					pstmt.addBatch();
					pstmt.executeBatch();
				}		

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM t1 WHERE a > ?")) {
					pstmt.setInt(1, 90);
					pstmt.execute();
				}		
				//conn.commit();

				try (Statement stmt = conn.createStatement()) {
					stmt.execute("alter table t1 add column if not exists c int null default(10)");					
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1(a, b, c) VALUES(?,?,?)")) {
					for (int i = 100000; i <= 10; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.setInt(3, i);
						pstmt.addBatch();						
					}
					pstmt.executeBatch();
				}

				try (Statement stmt = conn.createStatement()) {
					conn.setAutoCommit(false);
					stmt.execute("alter table t1 alter column c SET DATA TYPE BIGINT");
					stmt.execute("alter table t1 rename column c to d");
					stmt.execute("alter table t1 drop column d");
					stmt.execute("alter table t1 rename to t2");
					conn.commit();
				}
			}
		}

		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.executeQuery("SELECT * FROM t2");
			}
		}

		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("drop table t2");
			}
		}
		
	}

	private static void testDuckDBAppenderBasic() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.DuckDBAppender");

		Path dbPath = syncLiteDB.resolve("testDuckDBAppenderBasic.db");
		Path config = syncLiteLoggerConfig;	
		DuckDBAppender.initialize(dbPath, config, "testDuckDBAppenderBasic");

		String url = "jdbc:synclite_duckdb_appender:" + dbPath;
		{
			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
					stmt.executeUpdate("insert into t1 values(0,0)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("select a,b from t1")) {
					pstmt.executeQuery();
				}
				
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1(a, b) VALUES(?,?)")) {
					for (int i = 1; i <= 100; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}			
			}

			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.executeQuery("SELECT * FROM t1");
				}
			}
		}
	}

	private static void testDuckDBVector() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.DuckDB");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testDuckVector.db");
		Path config = syncLiteLoggerConfig;	
		DuckDB.initialize(dbPath, config, "testDuckDBBasic");

		String url = "jdbc:synclite_duckdb:" + dbPath;

		{

			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b float[])");
					stmt.executeUpdate("insert into t1 values(0,'[1,2,3]')");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1(a, b) VALUES(?,?)")) {
					for (int i = 1; i <= 100; ++i) {
						pstmt.setInt(1, i);
						pstmt.setObject(2, "[1,2,3]");
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}
			}
		}

	}

	private static void testDuckDBConcurrent() throws SQLException, ClassNotFoundException, InterruptedException {
		Class.forName("io.synclite.logger.DuckDB");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testDuckDBConcurrent.db");
		Path config = syncLiteLoggerConfig;	
		DuckDB.initialize(dbPath, config, "testDuckDBConcurrent");

		String url = "jdbc:synclite_duckdb:" + dbPath;

		DBThread th1 = new DBThread(url, "t1");
		DBThread th2 = new DBThread(url, "t2");
		DBThread th3 = new DBThread(url, "t3");

		th1.start();
		th2.start();
		th3.start();

		th1.join();
		th2.join();
		th3.join();
	}

	private static void testDuckDB() throws ClassNotFoundException, SQLException, InterruptedException {
		testDuckDBBasic();
		//testDuckDBVector();
		//testDuckDBConcurrent();
	}

	private static void testDuckDBAppender() throws ClassNotFoundException, SQLException, InterruptedException {
		testDuckDBAppenderBasic();
	}

	private static void testH2() throws ClassNotFoundException, SQLException, InterruptedException {
		testH2Basic();
		//testH2Concurrent();
	}

	private static void testH2Basic() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.H2");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testH2Basic.db");
		Path config = syncLiteLoggerConfig;	
		H2.initialize(dbPath, config, "testH2Basic");

		String url = "jdbc:synclite_h2:" + dbPath;
		{
			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {

					for (int i = 1; i <= 100; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();						
					}
					pstmt.executeBatch();
				}			

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE t1 set b = ? WHERE a < ?")) {
					pstmt.setInt(1, 1000);
					pstmt.setInt(2, 10);
					pstmt.addBatch();
					pstmt.executeBatch();
				}		

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM t1 WHERE a > ?")) {
					pstmt.setInt(1, 90);
					pstmt.execute();
				}		
				//conn.commit();
			}
		}

		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.executeQuery("SELECT * FROM t1");
			}
		}
	}



	private static void testHyperSQL() throws ClassNotFoundException, SQLException, InterruptedException {
		testHyperSQLBasic();
		//testH2Concurrent();
	}

	private static void testHyperSQLBasic() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.HyperSQL");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testHyperSQLBasic.db");
		Path config = syncLiteLoggerConfig;	
		HyperSQL.initialize(dbPath, config, "testHyperSQLBasic");

		String url = "jdbc:synclite_hsqldb:" + dbPath;
		{
			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {

					for (int i = 1; i <= 100; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();						
					}
					pstmt.executeBatch();
				}			

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE t1 set b = ? WHERE a < ?")) {
					pstmt.setInt(1, 1000);
					pstmt.setInt(2, 10);
					pstmt.addBatch();
					pstmt.executeBatch();
				}		

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM t1 WHERE a > ?")) {
					pstmt.setInt(1, 90);
					pstmt.execute();
				}		
				//conn.commit();
			}
		}

		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.executeQuery("SELECT * FROM t1");
			}
		}

		HyperSQL.closeAllDevices();
	}

	private static void testH2Concurrent() throws SQLException, ClassNotFoundException, InterruptedException {
		Class.forName("io.synclite.logger.H2");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testDuckDBConcurrent.db");
		Path config = syncLiteLoggerConfig;	
		DuckDB.initialize(dbPath, config, "testDuckDBConcurrent");

		String url = "jdbc:synclite_h2:" + dbPath;

		DBThread th1 = new DBThread(url, "t1");
		DBThread th2 = new DBThread(url, "t2");
		DBThread th3 = new DBThread(url, "t3");

		th1.start();
		th2.start();
		th3.start();

		th1.join();
		th2.join();
		th3.join();
	}

	private static void testStreaming() throws ClassNotFoundException, SQLException, InterruptedException {
		Class.forName("io.synclite.logger.Streaming");
		Path dbPath = syncLiteDB.resolve("testStreaming.db");
		String url = "jdbc:synclite_streaming:" + dbPath;
		{
			SyncLiteOptions options = new SyncLiteOptions();
			//options.setDestinationType(1, DestinationType.FS);
			//options.setLocalStageDirectory(1, Path.of("E:\\database\\stageDir"));
			//options.setDestinationType(2, DestinationType.FS);
			//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

			options.setDestinationType(1, DestinationType.FS);
			options.setLocalDataStageDirectory(1, syncLiteStage);
			options.setDeviceName("testStreaming");
			Streaming.initialize(dbPath, options);

			try (Connection conn = DriverManager.getConnection(url)) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("create table if not exists t1(a int, b int)");
					stmt.execute("refresh table t1(a int, b int)");
				}

				try (PreparedStatement pstmt = conn.prepareStatement("insert into t1 values(?,?)")) {
					pstmt.setInt(1, 1);
					pstmt.setInt(2, 1);
					pstmt.addBatch();

					pstmt.setInt(1, 2);
					pstmt.setInt(2, 2);
					pstmt.addBatch();

					pstmt.executeBatch();
				}
			}
		}
		{

			try (Connection conn = DriverManager.getConnection(url)) {
				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {
					for (int i = 0 ; i < 1000010; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();
					}
					pstmt.executeBatch();
				}			 
			}
		}

		/*
		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("COPY t1 from 'c:\\synclite\\database\\t1.csv';COPY t1 from 'c:\\\\synclite\\\\database\\\\t1.csv' ");
			}
		}*/
	}

	private static void testDerbyBasic() throws SQLException, ClassNotFoundException {
		Class.forName("io.synclite.logger.Derby");

		//Path dbPath = Path.of("C:\\synclite\\database\\db\\testTran.db");
		//Path dbPath = Path.of("C:\\Users\\arati\\synclite\\demo\\db\\1");
		//Path dbPath = Path.of("c:\\synclite\\python\\data\\t.db");
		//Path dbPath = Path.of("t.db");
		//SyncLiteOptions options = new SyncLiteOptions();
		//options.setLogSegmentFlushBatchSize(10);
		//options.setLocalDataDirectory(Path.of("E:\\database\\datadir"));
		//options.setLocalDataDirectory(Path.of("\\\\xyzO\\uploadDir"));
		//options.setLocalDataDirectory(Path.of("C:\\Users\\xyz\\OneDrive\\test"));
		//options.setAsyncLogging(false);
		//options.setDestinationType(1, DestinationType.FS);
		//options.setLocalStageDirectory(1, Path.of("E:\\database\\dataDir"));
		//options.setDestinationType(2, DestinationType.FS);
		//options.setLocalDataDirectory(2, Path.of("E:\\database\\dataDir1"));

		//SQLite.initialize(dbPath, options);
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"));
		//SQLite.initialize(dbPath, Path.of("C:\\Users\\xyz\\synclite\\demo\\db\\synclite_logger.conf"), "1");

		Path dbPath = syncLiteDB.resolve("testDerbyBasic.db");
		Path config = syncLiteLoggerConfig;	
		Derby.initialize(dbPath, config, "testDerbyBasic");

		String url = "jdbc:synclite_derby:" + dbPath;

		{

			try (Connection conn = DriverManager.getConnection(url)) {
				//conn.setAutoCommit(false);
				try (Statement stmt = conn.createStatement()) {					
					stmt.execute("create table t1(a int, b int)");
				} catch (SQLException e) {
					if (! e.getMessage().contains("already exists")) {
						throw e;
					}
				}

				try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO t1 VALUES(?,?)")) {

					for (int i = 1; i <= 100; ++i) {
						pstmt.setInt(1, i);
						pstmt.setInt(2, i);
						pstmt.addBatch();						
					}
					pstmt.executeBatch();
				}			

				try (PreparedStatement pstmt = conn.prepareStatement("UPDATE t1 set b = ? WHERE a < ?")) {
					pstmt.setInt(1, 1000);
					pstmt.setInt(2, 10);
					pstmt.addBatch();
					pstmt.executeBatch();
				}		

				try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM t1 WHERE a > ?")) {
					pstmt.setInt(1, 90);
					pstmt.execute();
				}		
				//conn.commit();
			}
		}

		try (Connection conn = DriverManager.getConnection(url)) {
			try (Statement stmt = conn.createStatement()) {
				stmt.executeQuery("SELECT * FROM t1");
			}			
		}
	}

	private static void testDerby() throws ClassNotFoundException, SQLException {
		testDerbyBasic();
	}

}

