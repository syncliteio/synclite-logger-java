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
Stream API sample via JPype.

This sample uses the Stream API for append-oriented event ingestion. It is the
API-level counterpart to the streaming-device JDBC sample and is meant for
workloads where new records are emitted continuously.

The distinction from Store API is that Stream API is not modeling mutable
business rows. It models event flow. INSERT remains the core operation,
selected DDL is available for schema evolution, and UPDATE or DELETE are
intentionally unsupported.
"""

from _common import start_jvm


def main():
    start_jvm()

    from java.nio.file import Path
    from java.sql import DriverManager, SQLException
    from java.util import LinkedHashMap, HashMap
    from io.synclite.logger import Streaming, SyncLiteStream

    db_path = Path.of("sample_stream_api_jpype.db")
    Streaming.initialize(db_path, Path.of("synclite_logger.conf"))

    stream = SyncLiteStream.open(db_path)

    cols = LinkedHashMap()
    cols.put("ts", "BIGINT")
    cols.put("event_type", "TEXT")
    cols.put("user_id", "TEXT")
    stream.createTable("events", cols)

    archive_cols = LinkedHashMap()
    archive_cols.put("id", "INT")
    archive_cols.put("note", "TEXT")
    stream.createTable("temp_events_archive", archive_cols)

    row = HashMap()
    row.put("ts", 1001)
    row.put("event_type", "SIGNUP")
    row.put("user_id", "user-10")
    stream.insert("events", row)

    row2 = HashMap()
    row2.put("ts", 1002)
    row2.put("event_type", "PURCHASE")
    row2.put("user_id", "user-11")
    row2.put("source", "web")
    stream.insert("events", row2)

    # Stream API also relies on JDBC for drop-column in this sample.
    conn = DriverManager.getConnection("jdbc:synclite_streaming:sample_stream_api_jpype.db")
    stmt = conn.createStatement()
    stmt.execute("ALTER TABLE events DROP COLUMN source")
    try:
        stmt.execute("UPDATE events SET event_type='X' WHERE user_id='user-10'")
    except SQLException as e:
        print("Expected UPDATE failure on Streaming device:", e.getMessage())
    try:
        stmt.execute("DELETE FROM events WHERE user_id='user-11'")
    except SQLException as e:
        print("Expected DELETE failure on Streaming device:", e.getMessage())
    stmt.close()
    conn.close()

    stream.dropTable("temp_events_archive")
    stream.close()
    Streaming.closeDevice(db_path)


if __name__ == "__main__":
    main()
