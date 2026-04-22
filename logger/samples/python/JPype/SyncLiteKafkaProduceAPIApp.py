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
KafkaProducer API sample via JPype.

This sample shows producer-style publishing through the SyncLite KafkaProducer
Java API from Python. The application deals with records and topics directly
instead of modeling messages as rows through a SQL surface.

That makes it the API-oriented counterpart to the SQL-based streaming samples.
Use this path when the application is already structured around Kafka producer
semantics and you want SyncLite persistence behind that API.
"""

from _common import start_jvm


def main():
    start_jvm()

    from java.util import Properties
    from org.apache.kafka.clients.producer import ProducerRecord
    from io.synclite.logger import KafkaProducer

    props = Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("device-path", "sample_kafka_api_jpype.db")
    props.put("device-type", "STREAMING")

    producer = KafkaProducer(props)
    producer.send(ProducerRecord("orders", "order-1", "{\"status\":\"created\"}"))
    producer.send(ProducerRecord("orders", "order-2", "{\"status\":\"confirmed\"}"))
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()