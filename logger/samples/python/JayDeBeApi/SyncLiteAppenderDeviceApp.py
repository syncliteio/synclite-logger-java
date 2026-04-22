# Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
#
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.
#
"""
Appender device sample (JayDeBeApi + JDBC URL).

Appender devices are aimed at write-heavy ingest workloads where the dominant
operation is appending new rows. They keep the calling pattern simple and focus
on durable INSERT-style logging.

Compared to a SQL device, this is intentionally narrower. If the workload needs
arbitrary SQL semantics and replay on a replica, use a SQL device instead.
"""

import jaydebeapi

# Appender default: SQLiteAppender.
# Replace for other appender engines:
# 1) Driver class: io.synclite.logger.SQLiteAppender -> DerbyAppender, DuckDBAppender, H2Appender, HyperSQLAppender
# 2) JDBC URL prefix:
#    jdbc:synclite_sqlite_appender: -> jdbc:synclite_derby_appender:, jdbc:synclite_duckdb_appender:, jdbc:synclite_h2_appender:, jdbc:synclite_hsqldb_appender:

props = {
    "config": "synclite_logger.conf",
    "device-name": "appender-sample"
}

conn = jaydebeapi.connect(
    "io.synclite.logger.SQLiteAppender",
    "jdbc:synclite_sqlite_appender:sample_appender_sqlite_py.db",
    props,
    "synclite-logger-<version>.jar"
)

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS feedback(rating INT, comment TEXT)")
# Keep this sample insert-focused to reflect appender-style usage.
cur.executemany("INSERT INTO feedback VALUES(?, ?)", [[4, "Excellent Product"], [5, "Outstanding Product"]])
cur.execute("close database sample_appender_sqlite_py.db")
cur.close()
conn.close()
