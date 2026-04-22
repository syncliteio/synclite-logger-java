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
Streaming device sample (JayDeBeApi + JDBC URL).

Streaming devices are for append-first event capture. The normal pattern is to
define the stream shape and keep inserting new records that represent events,
messages, or telemetry.

This is not a mutable relational surface. INSERT is the primary data path,
selected DDL is supported for schema evolution, and UPDATE or DELETE are shown
as expected failures.
"""

import time
import jaydebeapi

props = {
    "config": "synclite_logger.conf",
    "device-name": "streaming-sample"
}

conn = jaydebeapi.connect(
    "io.synclite.logger.Streaming",
    "jdbc:synclite_streaming:sample_streaming_py.db",
    props,
    "synclite-logger-<version>.jar"
)

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS events(ts BIGINT, event_type TEXT, user_id TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS temp_events_archive(id INT, note TEXT)")
cur.executemany(
    "INSERT INTO events VALUES(?, ?, ?)",
    [[int(time.time() * 1000), "CLICK", "user-1"], [int(time.time() * 1000), "VIEW", "user-2"]]
)
cur.execute("ALTER TABLE events ADD COLUMN source TEXT")
cur.execute("INSERT INTO events VALUES(?, ?, ?, ?)", [int(time.time() * 1000), "PURCHASE", "user-3", "mobile"])
cur.execute("ALTER TABLE events DROP COLUMN source")
cur.execute("DROP TABLE IF EXISTS temp_events_archive")

# Streaming is append-only from a DML perspective; UPDATE/DELETE should fail.
try:
    cur.execute("UPDATE events SET event_type='X' WHERE user_id='user-1'")
except Exception as e:
    print("Expected UPDATE failure on Streaming device:", e)

try:
    cur.execute("DELETE FROM events WHERE user_id='user-2'")
except Exception as e:
    print("Expected DELETE failure on Streaming device:", e)

cur.execute("close database sample_streaming_py.db")
cur.close()
conn.close()
