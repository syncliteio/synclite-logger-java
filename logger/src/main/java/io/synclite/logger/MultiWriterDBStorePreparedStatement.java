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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.sqlite.SQLiteConnection;

public class MultiWriterDBStorePreparedStatement extends SyncLiteStorePreparedStatement {

    private final PreparedStatement pstmt;

    public MultiWriterDBStorePreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
        super(conn, sql);
        pstmt = getConn().getNativeDBConnection().prepareStatement(sql);
    }

    @Override
    final protected MultiWriterDBStoreConnection getConn() {
        return ((MultiWriterDBStoreConnection) this.conn);
    }

    @Override
    final protected boolean pStmtExecute() throws SQLException {
        if (tableNameInDDL != null) {
            throw new SQLException("DDL statements not permitted with PreparedStatement");
        }
        boolean res = false;
        if ((batchQueryCount == 0) && (batch != null) && (paramCount > 0)) {
            for (int pos = 0; pos < paramCount; pos++) {
                Object o = batch[batchPos + pos];
                pstmt.setObject(pos + 1, o);
            }
            pstmt.addBatch();
            pstmt.executeBatch();
            res = false;
        } else {
            res = pstmt.execute();
        }
        return res;
    }

    @Override
    final protected ResultSet pStmtExecuteQuery() throws SQLException {
        if (tableNameInDDL != null) {
            throw new SQLException("DDL statements not permitted with executeQuery function");
        }
        return pstmt.executeQuery();
    }

    @Override
    final protected void pStmtAddBatch() throws SQLException {
        for (int pos = 0; pos < paramCount; pos++) {
            Object o = batch[batchPos + pos];
            pstmt.setObject(pos + 1, o);
        }
        pstmt.addBatch();
        ++batchQueryCount;
    }

    @Override
    final protected int[] pStmtExecuteBatch() throws SQLException {
        if (tableNameInDDL != null) {
            throw new SQLException("DDL statements not permitted with PreparedStatement");
        }
        return pstmt.executeBatch();
    }

    @Override
    final public void close() throws SQLException {
        super.close();
        if (this.pstmt != null) {
            pstmt.close();
        }
    }

    private SQLStager getCommandStager() {
        return getConn().getCommandStager();
    }

    @Override
    protected final void log() throws SQLException {
        long commitId = ((SyncLiteStoreConnection) this.conn).getCommitId();
        Object[] args = new Object[paramCount];
        for (int pos = 0; pos < paramCount; pos++) {
            args[pos] = batch[batchPos + pos];
        }
        getCommandStager().log(commitId, sql, args);
    }

    @Override
    protected final void log(Object[] args) throws SQLException {
        long commitId = ((SyncLiteStoreConnection) this.conn).getCommitId();
        if (batchQueryCount == 0) {
            getCommandStager().log(commitId, this.sql, args);
        } else if (batchQueryCount == 1) {
            getCommandStager().log(commitId, this.sql, args);
        } else {
            getCommandStager().log(commitId, null, args);
        }
    }
}
