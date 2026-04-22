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
SQL device sample (JayDeBeApi + JDBC URL).

A SQL device accepts general SQL and logs the statements as written. During
consolidation those statements are replayed on a replica, CDC is derived from
the replica, and that change stream is then applied to downstream destinations.

That is why SQL devices can support statements such as INSERT INTO ... SELECT
... FROM .... Store devices are narrower: they focus on operations where the
changed data is already explicit in the request so it can be propagated
directly.
"""

import jaydebeapi

# SQL-device default: SQLite.
# Replace for other SQL-device engines:
# 1) Driver class: io.synclite.logger.SQLite -> Derby, DuckDB, H2, HyperSQL
# 2) JDBC URL prefix:
#    jdbc:synclite_sqlite: -> jdbc:synclite_derby:, jdbc:synclite_duckdb:, jdbc:synclite_h2:, jdbc:synclite_hsqldb:

props = {
    "config": "synclite_logger.conf",
    "device-name": "txn-sample"
}

conn = jaydebeapi.connect(
    "io.synclite.logger.SQLite",
    "jdbc:synclite_sqlite:sample_txn_sqlite_py.db",
    props,
    "synclite-logger-<version>.jar"
)

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS feedback(rating INT, comment TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS temp_feedback_archive(id INT, note TEXT)")
cur.execute("INSERT INTO feedback VALUES(3, 'Good product')")

# SQL device supports transactional semantics (commit/rollback).
conn.jconn.setAutoCommit(False)
cur.execute("UPDATE feedback SET comment='Better product' WHERE rating=3")
cur.execute("INSERT INTO feedback VALUES(1, 'Poor product')")
cur.execute("DELETE FROM feedback WHERE rating=1")
conn.commit()
conn.jconn.setAutoCommit(True)

cur.executemany("INSERT INTO feedback VALUES(?, ?)", [[4, "Excellent Product"], [5, "Outstanding Product"]])

cur.execute("ALTER TABLE feedback ADD COLUMN source TEXT")
cur.execute("UPDATE feedback SET source='web' WHERE rating=3")
cur.execute("ALTER TABLE feedback DROP COLUMN source")
cur.execute("DROP TABLE IF EXISTS temp_feedback_archive")

cur.execute("close database sample_txn_sqlite_py.db")
cur.close()
conn.close()
