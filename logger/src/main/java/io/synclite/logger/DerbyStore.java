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
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.sqlite.SQLiteConnection;

public final class DerbyStore extends SyncLite {

    private final static String PREFIX = "jdbc:synclite_derby_store:";

    public static boolean isValidURL(String url) {
        return url != null && url.toLowerCase().startsWith(PREFIX);
    }

    @Override
    protected final SQLiteConnection createSyncLiteConnection(String url, Properties prop) throws SQLException {
        if (!isValidURL(url)) {
            return null;
        }
        url = url.trim();
        prop.put("original-db-url", url);
        prop.put("original-db-path", extractAddress(url, PREFIX));
        return new DerbyStoreConnection(url, extractAddress(url, PREFIX), prop);
    }

    public static final void initialize(Path dbPath) throws SQLException {
        SyncLite.initialize(DeviceType.DERBY_STORE, dbPath);
    }

    public static final void initialize(Path dbPath, String deviceName) throws SQLException {
        SyncLite.initialize(DeviceType.DERBY_STORE, dbPath, deviceName);
    }

    public static final void initialize(Path dbPath, SyncLiteOptions options) throws SQLException {
        SyncLite.initialize(DeviceType.DERBY_STORE, dbPath, options);
    }

    public static final void initialize(Path dbPath, SyncLiteOptions options, String deviceName) throws SQLException {
        SyncLite.initialize(DeviceType.DERBY_STORE, dbPath, options, deviceName);
    }

    public static final void initialize(Path dbPath, Path propsPath) throws SQLException {
        SyncLite.initialize(DeviceType.DERBY_STORE, dbPath, propsPath);
    }

    public static final void initialize(Path dbPath, Path propsPath, String deviceName) throws SQLException {
        SyncLite.initialize(DeviceType.DERBY_STORE, dbPath, propsPath, deviceName);
    }

    @Override
    protected String getPrefix() {
        return PREFIX;
    }

    @Override
    protected DBProcessor getDBProcessor() {
        return new DerbyProcessor();
    }

    @Override
    protected void validateLibs(Logger tracer) throws SQLException {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            tracer.error("Failed to load sqlite jdbc driver : " + e.getMessage());
            throw new SQLException("Failed to load sqlite jdbc driver");
        }
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        } catch (ClassNotFoundException e) {
            tracer.error("Failed to load derby jdbc driver : " + e.getMessage());
            throw new SQLException("Failed to load derby jdbc driver");
        }
    }

    @Override
    protected void setDeviceTypeInOptions(SyncLiteOptions options) throws SQLException {
        options.setDeviceType(DeviceType.DERBY_STORE);
    }

    @Override
    protected void getOrCreateLoggerInstace(Path dbPath, SyncLiteOptions options, Logger tracer) throws SQLException {
        SyncTxnLogger.getInstance(dbPath, options, tracer);
    }

    /** SQL type used for String-valued columns auto-added by this backend. */
    public static String defaultStringType() { return "VARCHAR(32672)"; }

    public static SyncLiteStore open(Path dbPath) throws SQLException {
        return new SyncLiteStore(dbPath, PREFIX, defaultStringType());
    }

    public static SyncLiteStore openUnlogged(Path dbPath) throws SQLException {
        return new SyncLiteStore(dbPath, PREFIX, defaultStringType(), true);
    }

    public static SyncLiteStore open(Path dbPath, SyncLiteOptions options) throws SQLException {
        initialize(dbPath, options);
        return new SyncLiteStore(dbPath, PREFIX, defaultStringType());
    }
}
