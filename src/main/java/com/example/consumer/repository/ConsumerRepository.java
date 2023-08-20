package com.example.consumer.repository;

import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


@Service
public class ConsumerRepository {
    private static final String TOPIC = "my-kafka-topic";
    //private static final String BOOTSTRAP_SERVERS = "172.18.0.4:29092";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    @PostConstruct
    private static void consume() {
        // Create configuration options for our consumer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        // The group ID is a unique identified for each consumer group
        props.setProperty("group.id", "my-group-id");
        // Since our producer uses a string serializer, we need to use the corresponding
        // deserializer
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        // Every time we consume a message from kafka, we need to "commit" - that is, acknowledge
        // receipts of the messages. We can set up an auto-commit at regular intervals, so that
        // this is taken care of in the background
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");

        // Since we need to close our consumer, we can use the try-with-resources statement to
        // create it
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Subscribe this consumer to the same topic that we wrote messages to earlier
            consumer.subscribe(Arrays.asList(TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
	    for (ConsumerRecord<String, String> record : records) {
		    System.out.printf("received message: %s\n", record.value());
	    }
	    // run an infinite loop where we consume and print new messages to the topic
            /*
	    while (true) {
                // The consumer.poll method checks and waits for any new messages to arrive for the
                // subscribed topic
                // in case there are no messages for the duration specified in the argument (1000 ms
                // in this case), it returns an empty list
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("received message: %s\n", record.value());
                }
            }
	    */
        }
    }

}
