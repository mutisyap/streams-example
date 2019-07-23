package tech.meliora.academy.streamsexample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class TestProducer extends Thread {
    private static final String KAFKA_SERVER_URL = "http://51.15.233.87:9092";
    private static final String CLIENT_ID = "streams-pipe";
    private final KafkaProducer<String, Map> producer;
    private final String topic;

    public TestProducer(String topic) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, CLIENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "tech.meliora.academy.streamsexample.serializer.HashMapSerializer");

//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        this.producer = new KafkaProducer<>(props);
        this.topic = topic;

        System.out.println("Initialized");
    }

    @Override
    public void run() {
        while (true) {
            try {

                Map<String, String> message = new HashMap<>();
                // System.out.println("Testing");
                String messageNumber = UUID.randomUUID().toString();

                message.put("id", messageNumber);
                message.put("name", messageNumber + " - Message");

                try {
                    // System.out.println("About to send - " +messageNumber + ": " + messageStr);
                    RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, messageNumber, message)).get();
                    System.out.println("Successfully Sent. Metadata = " + metadata);
                    // System.out.println(messageNumber + ": " + messageStr);
                } catch (InterruptedException | ExecutionException e) {
                    System.out.println("Encountered Error: " + e.getLocalizedMessage());
                    e.printStackTrace();
                }
            } catch (Exception ex) {
                break;
            }


        }
    }
}
