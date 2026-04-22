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
Streaming device sample via JPype and JDBC SQL.

Streaming devices are for append-first event capture. The normal pattern is to
define the stream shape and keep inserting new records that represent events,
messages, or telemetry.

This is not a mutable relational surface. INSERT is the primary data path,
selected DDL is supported for schema evolution, and UPDATE or DELETE are shown
as expected failures.
"""

from _common import start_jvm


def main():
    start_jvm()

    from java.nio.file import Path
    from java.sql import DriverManager, SQLException
    from io.synclite.logger import Streaming

    db_path = Path.of("sample_streaming_jpype.db")
    Streaming.initialize(db_path, Path.of("synclite_logger.conf"))

    conn = DriverManager.getConnection("jdbc:synclite_streaming:sample_streaming_jpype.db")
    stmt = conn.createStatement()
    stmt.execute("CREATE TABLE IF NOT EXISTS events(ts BIGINT, event_type TEXT, user_id TEXT)")
    stmt.execute("CREATE TABLE IF NOT EXISTS temp_events_archive(id INT, note TEXT)")
    stmt.execute("INSERT INTO events VALUES(1001, 'CLICK', 'user-1')")
    stmt.execute("INSERT INTO events VALUES(1002, 'VIEW', 'user-2')")
    stmt.execute("ALTER TABLE events ADD COLUMN source TEXT")
    stmt.execute("INSERT INTO events VALUES(1003, 'PURCHASE', 'user-3', 'mobile')")
    stmt.execute("ALTER TABLE events DROP COLUMN source")
    stmt.execute("DROP TABLE IF EXISTS temp_events_archive")

    # By design, streaming device does not support row mutation/deletion.
    try:
        stmt.execute("UPDATE events SET event_type='X' WHERE user_id='user-1'")
    except SQLException as e:
        print("Expected UPDATE failure on Streaming device:", e.getMessage())

    try:
        stmt.execute("DELETE FROM events WHERE user_id='user-2'")
    except SQLException as e:
        print("Expected DELETE failure on Streaming device:", e.getMessage())

    stmt.close()
    conn.close()
    Streaming.closeDevice(db_path)


if __name__ == "__main__":
    main()
