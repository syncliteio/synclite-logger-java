import java.nio.file.Path;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;

import io.synclite.logger.KafkaProducer;

/**
 * KafkaProducer API sample.
 *
 * This sample shows producer-style publishing through the SyncLite KafkaProducer
 * API. The application deals with records and topics directly instead of
 * modeling messages as rows through a SQL surface.
 *
 * That makes it the API-oriented counterpart to the SQL-based streaming samples.
 * Use this path when the application is already structured around Kafka producer
 * semantics and you want SyncLite persistence behind that API.
 */
public class SyncLiteKafkaProduceApp {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("device-path", Path.of("sample_kafka_device.db").toAbsolutePath().toString());
        props.put("device-type", "STREAMING");

        try (KafkaProducer producer = new KafkaProducer(props)) {
            producer.send(new ProducerRecord<>("orders", "order-1", "{\"status\":\"created\"}"));
            producer.send(new ProducerRecord<>("orders", "order-2", "{\"status\":\"confirmed\"}"));
            producer.flush();
        }
    }
}
