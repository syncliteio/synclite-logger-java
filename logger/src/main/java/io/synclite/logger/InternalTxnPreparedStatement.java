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
import org.sqlite.jdbc4.JDBC4PreparedStatement;

public class InternalTxnPreparedStatement extends JDBC4PreparedStatement {

	private final static String mockSql = "Select 1";
	private final String internalSql;
	public InternalTxnPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
		super(conn, mockSql);
		SyncLiteUtils.checkInternalSql(sql);
		this.internalSql = sql;		
	}
	
	@Override
    public final boolean execute() throws SQLException {
    	SyncLiteUtils.checkAndExecuteInternalTransactionalSql(internalSql);
    	return false;
    }
    @Override
    public final int[] executeBatch() throws SQLException {
    	execute();
    	return new int[1];
    }
}
