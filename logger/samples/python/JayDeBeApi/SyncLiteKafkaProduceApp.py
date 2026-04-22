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
Kafka-produce style sample using a Streaming device (JayDeBeApi).

This sample models Kafka-style messages as rows in a streaming table. It is the
SQL-oriented Python equivalent when the bridge is JDBC-based rather than direct
Java API access.

It is not using the KafkaProducer API itself. If you want direct Java producer
semantics from Python, the JPype KafkaProducer sample is the better match.
"""

import json
import time
import jaydebeapi

# Python equivalent of Kafka produce flow using a streaming table.
props = {
    "config": "synclite_logger.conf",
    "device-name": "kafka-produce-sample"
}

conn = jaydebeapi.connect(
    "io.synclite.logger.Streaming",
    "jdbc:synclite_streaming:sample_kafka_py.db",
    props,
    "synclite-logger-<version>.jar"
)

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS kafka_messages(ts BIGINT, topic TEXT, msg_key TEXT, msg_value TEXT)")
messages = [
    [int(time.time() * 1000), "orders", "order-1", json.dumps({"status": "created"})],
    [int(time.time() * 1000), "orders", "order-2", json.dumps({"status": "confirmed"})],
]
cur.executemany("INSERT INTO kafka_messages VALUES(?, ?, ?, ?)", messages)
cur.execute("close database sample_kafka_py.db")
cur.close()
conn.close()