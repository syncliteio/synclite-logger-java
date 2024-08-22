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

import org.sqlite.SQLiteConnection;

public class StreamingPreparedStatement extends TelemetryPreparedStatement {

	public StreamingPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
		super(conn, sql);
	}

	@Override
    final protected StreamingConnection getConn() {
        return ((StreamingConnection ) this.conn);
    }

	private SQLStager getCommandStager() {
		return getConn().getCommandStager();
	}

	@Override
	protected final void log(Object[] args) throws SQLException {
		long commitId = ((TelemetryConnection ) this.conn).getCommitId();
		if (batchQueryCount == 0) {			
			getCommandStager().log(commitId, this.sql, args);
		} else if (batchQueryCount == 1){
			getCommandStager().log(commitId, this.sql, args);
		} else {
			getCommandStager().log(commitId, null, args);
		}
	}
}
