package tech.meliora.academy.streamsexample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class StreamsExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamsExampleApplication.class, args);

        String topic = "sample";
        List<TestProducer> testProducers = new ArrayList<>();

        for (int i = 0; i <= 100; i++) {
            TestProducer testProducer = new TestProducer(topic);
            testProducers.add(testProducer);
        }

        for (int i = 0; i <= 100; i++) {
            TestProducer testProducer = testProducers.get(i);
            testProducer.start();
        }
    }

}
