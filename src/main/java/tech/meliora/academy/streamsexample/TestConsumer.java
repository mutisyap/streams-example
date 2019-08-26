package tech.meliora.academy.streamsexample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TestConsumer extends Thread {
    private static final String KAFKA_SERVER_URL = "http://[IP]:[PORT]";
    private static final String CLIENT_ID = "test-consumer-2";
    private final KafkaConsumer<String, Map> consumer;
    private final String topic;

    public TestConsumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "tech.meliora.academy.streamsexample.serializer.HashMapDeserializer");


        consumer = new KafkaConsumer<>(properties);

        this.topic = topic;
    }

    @Override
    public void run() {
        consumer.subscribe((Collections.singletonList(this.topic)));


        while (true) {
            try {

                ConsumerRecords<String, Map> records = consumer.poll(1000);
                for (ConsumerRecord<String, Map> consumerRecord : records) {
                    System.out.println("Received Message: (" + consumerRecord.key() + "," + consumerRecord.value() + ") at offset " + consumerRecord.offset());
                }

                consumer.commitSync();
            } catch (Exception ex) {
                System.out.println("Encountered Exception : " + ex +". Exiting");
                break;
            }
        }
    }
}
