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