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
import java.sql.SQLException;
import java.util.List;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4PreparedStatement;

public class TelemetryPreparedStatement extends JDBC4PreparedStatement {
	private SQLLogger sqlLogger;
	private boolean isDDL = false;
	public TelemetryPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
		super(conn, sql);
		List<String> subSqls = SyncLiteUtils.splitSqls(sql);
		if (subSqls.size() > 1) {
			throw new SQLException("SyncLite Telemetry supports a single SQL statement as part of a PreparedStatement, multiple specified  : " + sql);			
		}
		String stippedSql = subSqls.get(0).strip();
		String[] tokens = stippedSql.split("\\s+");		
		if (tokens[0].equalsIgnoreCase("INSERT") && tokens[1].equalsIgnoreCase("INTO")) {
			SyncLiteUtils.validateInsertForTelemetryAndAppender(stippedSql);
		} else if (tokens[0].equalsIgnoreCase("UPDATE")) {
			SyncLiteUtils.validateUpdateForTelemetryAndAppender(stippedSql);			
		} else if (tokens[0].equalsIgnoreCase("DELETE") && tokens[1].equalsIgnoreCase("FROM")) {
			SyncLiteUtils.validateDeleteForTelemetryAndAppender(stippedSql);			
		} else if ((tokens[0].equalsIgnoreCase("CREATE") || tokens[0].equalsIgnoreCase("DROP") || tokens[0].equalsIgnoreCase("ALTER")) &&
				(tokens[1].equalsIgnoreCase("TABLE"))
				) {
			this.isDDL = true;
		} else {
			throw new SQLException("SyncLite Telemetry : Unsupported SQL : " + sql);
		}
		this.sqlLogger = EventLogger.findInstance(getConn().getPath());
	}

	protected TelemetryConnection getConn() {
		return ((TelemetryConnection ) this.conn);
	}

	private final void log() throws SQLException {
		Object[] args = new Object[paramCount];
		for (int pos=0; pos < paramCount; pos++) {
			args[pos] = batch[batchPos + pos];
		}
		log(args);
	}

	protected void log(Object[] args) throws SQLException {
		long commitId = ((TelemetryConnection ) this.conn).getCommitId();
		if (batchQueryCount == 0) {
			sqlLogger.log(commitId, this.sql, args);
		} else if (batchQueryCount == 1){
			sqlLogger.log(commitId, this.sql, args);
		} else {
			sqlLogger.log(commitId, null, args);
		}
	}

	@Override
	public final boolean execute() throws SQLException {
		boolean result = false;
		if (this.isDDL) {
			result = super.execute();
			log();
			processCommit();
			return result;
		} else {
			if (batchQueryCount == 0) {
				log();
			}
			processCommit();
			batchQueryCount = 0;
			result = true;
		}
		return result;
	}

	private final void processCommit() throws SQLException {
		if (getConn().getUserAutoCommit() == true) {
			getConn().commit();
		}
	}

	@Override
	public final int[] executeBatch() throws SQLException {
		if (this.isDDL) {
			execute();
			log();
			processCommit();
		} else {
			if (batchQueryCount == 0) {
				log();
			}
			processCommit();
			batchQueryCount = 0;
		}
		return new int[] {};
	}

	@Override
	public final void addBatch() throws SQLException {
		//super.addBatch();
		++batchQueryCount;
		log();
		//super.clearBatch();
	}

}
