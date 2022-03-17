package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    private final static String KAFKA_TOPIC="lyd-test-kafka";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop01:9093");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            System.out.println(i);
            producer.send(new ProducerRecord<>(KAFKA_TOPIC, String.format("key-%s", i), String.format("value-%s", i)));
        }

        producer.close();
    }
}
