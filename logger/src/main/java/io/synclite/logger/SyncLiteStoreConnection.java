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

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.sqlite.SQLiteException;
import org.sqlite.jdbc4.JDBC4Connection;

public class SyncLiteStoreConnection extends JDBC4Connection {

    public static final String PREFIX = "jdbc:synclite_sqlite_store:";
    public static final String updateCommitLoggerSql = "UPDATE synclite_txn SET commit_id = ?, operation_id = ?";
    private boolean userAutoCommit;
    private PreparedStatement commitLoggerPstmt;
    protected Path path;
    protected long commitId;
    protected TxnLogger sqlLogger;
    private boolean ready = false;
    protected Properties props;

    public SyncLiteStoreConnection(String url, String fileName, Properties prop) throws SQLException {
        super(url, fileName, prop);
        this.props = prop;
        initPath(fileName);
        this.userAutoCommit = true;
        this.sqlLogger = (TxnLogger) SQLLogger.findInstance(path);
        if (this.sqlLogger == null) {
            if (prop != null) {
                initDevice(prop);
                this.sqlLogger = (TxnLogger) SQLLogger.findInstance(path);
            } else {
                initDeviceWithoutProps();
                this.sqlLogger = (TxnLogger) SQLLogger.findInstance(path);
            }
            if (this.sqlLogger == null) {
                throw new SQLException("SyncLite device at path " + path + " not initialized. Please initialize the device first.");
            }
        } else {
            if (!this.sqlLogger.isLoggerHealthy()) {
                throw new SQLException("SyncLite logger is not healthy for device : " + path
                        + ". Please check device trace file for more details. Please close and initialize the device again.");
            }
        }
        if (this.props != null) {
            cleanUpProps();
        }
        initConn();
        this.commitId = this.sqlLogger.getNextCommitID();
        this.ready = true;
    }

    private final void cleanUpProps() {
        props.remove("config");
        props.remove("device-name");
    }

    final void initConn() throws SQLException {
        doInitConn();
        prepareCommitLoggerPStmt();
    }

    protected void prepareCommitLoggerPStmt() throws SQLException {
        this.commitLoggerPstmt = super.prepareStatement(updateCommitLoggerSql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    protected void initDevice(Properties prop) throws SQLException {
        Object configPathObj = prop.get("config");
        Object deviceName = prop.get("device-name");
        if (configPathObj != null) {
            if (deviceName != null) {
                SQLiteStore.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
            } else {
                SQLiteStore.initialize(this.path, Path.of(configPathObj.toString()));
            }
        } else {
            if (deviceName != null) {
                SQLiteStore.initialize(this.path, deviceName.toString());
            } else {
                SQLiteStore.initialize(this.path);
            }
        }
    }

    protected void initDeviceWithoutProps() throws SQLException {
        SQLiteStore.initialize(this.path);
    }

    protected void doInitConn() throws SQLException {
        connAutoCommit(false);
    }

    protected void connAutoCommit(boolean b) throws SQLException {
        super.setAutoCommit(b);
    }

    protected Statement connCreateStatement() throws SQLException {
        return new SyncLiteStoreStatement(this);
    }

    protected PreparedStatement connPrepareStatement(SyncLiteStoreConnection conn, String sql) throws SQLException {
        return new SyncLiteStorePreparedStatement(this, sql);
    }

    // Package-private: creates a PreparedStatement that skips DML validation.
    // Only callable by classes in this package (e.g. SyncLiteStore).
    final PreparedStatement prepareTrustedStatement(String sql) throws SQLException {
        checkOpen();
        return connPrepareStatementTrusted(this, sql);
    }

    protected PreparedStatement connPrepareStatementTrusted(SyncLiteStoreConnection conn, String sql) throws SQLException {
        return new SyncLiteStorePreparedStatement(this, sql, true);
    }

    protected void connCommit() throws SQLException {
        super.commit();
    }

    protected void connRollback() throws SQLException {
        super.rollback();
    }

    protected void initPath(String fileName) {
        this.path = Path.of(fileName);
    }

    final Path getPath() {
        return this.path;
    }

    final long getCommitId() {
        return commitId;
    }

    final long getOperationId() {
        return this.sqlLogger.getOperationID();
    }

    PreparedStatement prepareUnloggedStatement(String sql) throws SQLException {
        return new SyncLiteUnloggedPreparedStatement(this, sql);
    }

    @Override
    public final Statement createStatement(int rst, int rsc, int rsh) throws SQLException {
        if (!this.ready) {
            return super.createStatement(rst, rsc, rsh);
        }
        checkOpen();
        checkCursor(rst, rsc, rsh);
        return connCreateStatement();
    }

    @Override
    public final PreparedStatement prepareStatement(String sql, int rst, int rsc, int rsh) throws SQLException {
        if (!this.ready) {
            return super.prepareStatement(sql, rst, rsc, rsh);
        }
        checkOpen();
        checkCursor(rst, rsc, rsh);
        PreparedStatement pstmt = null;
        try {
            return connPrepareStatement(this, sql);
        } catch (SQLException e) {
            if (e.getMessage().contains("Unsupported SQL") || e.getMessage().contains("Parse error")) {
                try {
                    pstmt = new InternalAppenderPreparedStatement(this, sql);
                } catch (SQLiteException e1) {
                    if (e1.getMessage().contains("Unsupported SQL")) {
                        throw e;
                    }
                }
            } else {
                throw e;
            }
        }
        return pstmt;
    }

    @Override
    public void commit() throws SQLException {
        if (!this.sqlLogger.hasPendingLogs()) {
            return;
        }
        recordCommit();
        this.sqlLogger.flush(commitId);
        connCommit();
        this.sqlLogger.logCommitAndFlush(commitId);
        this.commitId = this.sqlLogger.getNextCommitID();
    }

    @Override
    public void rollback() throws SQLException {
        this.sqlLogger.flush(commitId);
        connRollback();
        this.sqlLogger.logRollbackAndFlush(commitId);
        this.commitId = this.sqlLogger.getNextCommitID();
    }

    protected void recordCommit() throws SQLException {
        commitLoggerPstmt.setLong(1, commitId);
        commitLoggerPstmt.setLong(2, this.sqlLogger.getOperationID());
        commitLoggerPstmt.execute();
    }

    final PreparedStatement getPstmt() {
        return commitLoggerPstmt;
    }

    @Override
    public final void setAutoCommit(boolean ac) throws SQLException {
        this.userAutoCommit = ac;
    }

    final boolean getUserAutoCommit() {
        return userAutoCommit;
    }

    protected final void superCommit() throws SQLException {
        super.commit();
    }

    protected PreparedStatement validateSQL(String sql) throws SQLException {
        return super.prepareStatement(sql);
    }
}
