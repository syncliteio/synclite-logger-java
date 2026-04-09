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

public class SyncLiteStorePreparedStatement extends JDBC4PreparedStatement {

    protected SQLLogger sqlLogger;
    protected boolean hasSpecialPositionalArg = false;
    private long processedRowCount = 0;
    protected String tableNameInDDL;

    public SyncLiteStorePreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
        super(conn, sql);
        String strippedSql = sql.strip();
        String tokens[] = strippedSql.split("\\s+");
        if (tokens[0].equalsIgnoreCase("INSERT") && tokens[1].equalsIgnoreCase("INTO")) {
            SyncLiteUtils.validateInsertForTelemetryAndAppender(strippedSql);
        } else if ((tokens[0].equalsIgnoreCase("CREATE") || tokens[0].equalsIgnoreCase("DROP") || tokens[0].equalsIgnoreCase("ALTER"))
                && (tokens[1].equalsIgnoreCase("TABLE"))) {
            // Allowed DDL
        } else if (tokens[0].equalsIgnoreCase("SELECT")) {
            // Allowed
        } else if (tokens[0].equalsIgnoreCase("UPDATE")) {
            SyncLiteUtils.validateUpdateForTelemetryAndAppender(strippedSql);
        } else if (tokens[0].equalsIgnoreCase("DELETE")) {
            SyncLiteUtils.validateDeleteForTelemetryAndAppender(strippedSql);
        } else {
            throw new SQLException("Unsupported SQL: SyncLite store device does not allow SQL : " + sql
                    + ". Allowed SQLs are CREATE TABLE, DROP TABLE, ALTER TABLE, INSERT INTO, UPDATE, DELETE, SELECT");
        }
        this.sqlLogger = EventLogger.findInstance(getConn().getPath());
        this.tableNameInDDL = SyncLiteUtils.getTableNameFromDDL(sql);
    }

    // Package-private: for use by internal API layers (e.g. SyncLiteStore) that generate SQL
    // programmatically. Skips DML validation since the SQL is known-good by construction.
    // External callers in other packages cannot reach this constructor.
    SyncLiteStorePreparedStatement(SQLiteConnection conn, String sql, boolean trusted) throws SQLException {
        super(conn, sql);
        this.sqlLogger = EventLogger.findInstance(getConn().getPath());
        this.tableNameInDDL = SyncLiteUtils.getTableNameFromDDL(sql);
    }

    protected SyncLiteStoreConnection getConn() {
        return ((SyncLiteStoreConnection) this.conn);
    }

    protected void log() throws SQLException {
        Object[] args = new Object[paramCount];
        for (int pos = 0; pos < paramCount; pos++) {
            args[pos] = batch[batchPos + pos];
        }
        log(args);
    }

    protected void log(Object[] args) throws SQLException {
        long commitId = ((SyncLiteStoreConnection) this.conn).getCommitId();
        if (batchQueryCount == 0) {
            sqlLogger.log(commitId, this.sql, args);
        } else if (batchQueryCount == 1) {
            sqlLogger.log(commitId, this.sql, args);
        } else {
            sqlLogger.log(commitId, null, args);
        }
    }

    @Override
    public final boolean execute() throws SQLException {
        int cachedBatchQueryCount = batchQueryCount;
        this.processedRowCount += cachedBatchQueryCount;
        boolean result = pStmtExecute();
        if (cachedBatchQueryCount == 0) {
            log();
        }
        processCommit();
        return result;
    }

    @Override
    public final int executeUpdate() throws SQLException {
        execute();
        return getUpdateCount();
    }

    private final void processCommit() throws SQLException {
        if (getConn().getUserAutoCommit() == true) {
            getConn().commit();
        }
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        int cachedBatchQueryCount = batchQueryCount;
        this.processedRowCount += cachedBatchQueryCount;
        int[] result = pStmtExecuteBatch();
        if (cachedBatchQueryCount == 0) {
            log();
        }
        processCommit();
        return result;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return pStmtExecuteQuery();
    }

    protected ResultSet pStmtExecuteQuery() throws SQLException {
        return super.executeQuery();
    }

    @Override
    public final void addBatch() throws SQLException {
        pStmtAddBatch();
        log();
    }

    protected boolean pStmtExecute() throws SQLException {
        return superExecute();
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
}
