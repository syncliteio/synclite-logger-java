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
import java.sql.ResultSet;
import java.sql.SQLException;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4PreparedStatement;

public class SyncLitePreparedStatement extends JDBC4PreparedStatement {

    protected SQLLogger sqlLogger;
    protected String tableNameInDDL;
    public SyncLitePreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
        super(conn, sql);
        if (SyncLiteUtils.splitSqls(sql).size() > 1) {
            throw new SQLException("SyncLite supports a single SQL statement as part of a PreparedStatement, multiple specified  : " + sql);
        }
        this.sqlLogger = AsyncTxnLogger.findInstance(getConn().getPath());
        this.tableNameInDDL = SyncLiteUtils.getTableNameFromDDL(sql);
    }

    protected SyncLiteConnection getConn() {
        return ((SyncLiteConnection ) this.conn);
    }

    protected void log() throws SQLException {
        long commitId = ((SyncLiteConnection ) this.conn).getCommitId();
        Object[] args = new Object[paramCount];
        for (int pos=0; pos < paramCount; pos++) {
            args[pos] = batch[batchPos + pos];
        }
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
        boolean result= pStmtExecute();
        if (batchQueryCount == 0) {
            log();
        }
        processCommit();
        return result;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
    	return pStmtExecuteQuery();
    }
    
    private final void processCommit() throws SQLException {
        if (getConn().getUserAutoCommit() == true) {
            getConn().commit();
        }
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        int[] result = pStmtExecuteBatch();
        processCommit();
        return result;
    }

    @Override
    public final void addBatch() throws SQLException {
    	pStmtAddBatch();
    	log();
    }
    
    protected boolean pStmtExecute() throws SQLException {
    	return superExecute();
    }

    protected ResultSet pStmtExecuteQuery() throws SQLException {
    	return super.executeQuery();
    }

    final boolean superExecute() throws SQLException {
    	return super.execute();
    }    

    final boolean superExecute(String sql) throws SQLException {
    	return super.execute(sql);
    }    

    protected void pStmtAddBatch() throws SQLException {
        super.addBatch();
    }
    
    protected int[] pStmtExecuteBatch() throws SQLException {
        return super.executeBatch();
    }
    
    @Override
    public void close() throws SQLException {
    	super.close();
    }
    
    protected SyncLitePreparedStatement getSyncLitePreparedStatement() {
    	return this;
    }
}
