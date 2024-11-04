package org.colak.poll;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
class ConsumerPollTest {

    private static final String TOPIC_NAME = "demo_topic";

    private KafkaConsumer<String, String> kafkaConsumer;

    public static void main(String[] args) {
        ConsumerPollTest consumerPollTest = new ConsumerPollTest();
        consumerPollTest.start();
    }

    private void start() {
        kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        addShutdownHook();
        poll();
    }

    private void poll() {
        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : records) {
                processEvent(consumerRecord);
            }
        }
    }

    private void processEvent(ConsumerRecord<String, String> consumerRecord) {
        log.info("Received Key : {} Value :{}", consumerRecord.key(), consumerRecord.value());
        log.info("Partition : {} Offset : {}", consumerRecord.partition(), consumerRecord.offset());
    }

    private void addShutdownHook() {
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown");
            // See https://medium.com/@pravvich/apache-kafka-guide-15-java-api-consumer-group-fbbf49f8513b
            // wakeup() that is often used to interrupt and wake up a blocking Kafka consumer.
            // This method is typically used to gracefully shut down a consumer.
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException | WakeupException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }

}
