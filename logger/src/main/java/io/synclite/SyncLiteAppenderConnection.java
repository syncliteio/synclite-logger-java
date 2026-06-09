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

package io.synclite;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SyncLiteAppenderConnection extends SyncLiteStoreConnection {

    public static final String PREFIX = "jdbc:synclite_sqlite_appender:";

    public SyncLiteAppenderConnection(String url, String fileName, Properties prop) throws SQLException {
        super(url, fileName, prop);
    }

    @Override
    protected Statement connCreateStatement() throws SQLException {
        return new SyncLiteAppenderStatement(this);
    }

    @Override
    protected PreparedStatement connPrepareStatement(SyncLiteStoreConnection conn, String sql) throws SQLException {
        return new SyncLiteAppenderPreparedStatement(this, sql);
    }

    @Override
    protected void initDevice(Properties prop) throws SQLException {
        Object configPathObj = prop.get("config");
        Object deviceName = prop.get("device-name");
        if (configPathObj != null) {
            if (deviceName != null) {
                SQLite.initialize(this.path, Path.of(configPathObj.toString()), deviceName.toString());
            } else {
                SQLite.initialize(this.path, Path.of(configPathObj.toString()));
            }
        } else {
            if (deviceName != null) {
                SQLite.initialize(this.path, deviceName.toString());
            } else {
                SQLite.initialize(this.path);
            }
        }
    }

    @Override
    protected void initDeviceWithoutProps() throws SQLException {
        SQLite.initialize(this.path);
    }
}

