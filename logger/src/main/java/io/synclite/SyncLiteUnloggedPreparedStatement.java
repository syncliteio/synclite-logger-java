/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 */
package io.synclite;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4PreparedStatement;

/**
 * PreparedStatement used for unlogged writes on SyncLite store devices.
 *
 * This class delegates execution to the underlying JDBC PreparedStatement
 * implementation but avoids any SyncLite logging and ensures the native
 * (wrapper) connection is committed when user-autocommit mode is enabled.
 */
public class SyncLiteUnloggedPreparedStatement extends JDBC4PreparedStatement {

    public SyncLiteUnloggedPreparedStatement(SQLiteConnection conn, String sql) throws SQLException {
        super(conn, sql);
    }

    protected SyncLiteStoreConnection getConn() {
        return ((SyncLiteStoreConnection) this.conn);
    }

    @Override
    public final boolean execute() throws SQLException {
        boolean result = super.execute();
        processCommit();
        return result;
    }

    @Override
    public final int executeUpdate() throws SQLException {
        int rc = super.executeUpdate();
        processCommit();
        return rc;
    }

    @Override
    public final int[] executeBatch() throws SQLException {
        int[] r = super.executeBatch();
        processCommit();
        return r;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery();
    }

    @Override
    public final void addBatch() throws SQLException {
        super.addBatch();
    }

    private final void processCommit() throws SQLException {
        if (getConn().getUserAutoCommit() == true) {
            getConn().superCommit();
        }
    }

}
