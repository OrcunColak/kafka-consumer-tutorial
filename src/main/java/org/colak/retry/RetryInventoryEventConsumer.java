package org.colak.retry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// See https://medium.com/@swatikpl44/implementing-retry-mechanism-for-events-using-kafka-3a849eac9f2a
public class RetryInventoryEventConsumer {

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    private static final String TOPIC_NAME = "order_events_retry";
    private static final String DLQ_TOPIC_NAME = "order_events_dlq";

    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) {
        RetryInventoryEventConsumer inventoryEventConsumer = new RetryInventoryEventConsumer();
        inventoryEventConsumer.start();
    }


    private void start() {
        consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        this.producer = createProducer();
        poll();

    }


    private void poll() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                int retryAttempt = extractRetryCount(value);
                try {
                    processEvent(record);
                } catch (Exception e) {
                    handleRetry(record, retryAttempt + 1);
                }
            }
        }
    }

    private void processEvent(ConsumerRecord<String, String> record) throws Exception {
        System.out.println("Processing event: " + record.value());
        // Simulate a failure
        if (record.value().contains("fail")) {
            throw new Exception("Simulated processing error");
        }
        System.out.println("Event processed successfully");
    }

    private void handleRetry(ConsumerRecord<String, String> record, int attempt) {
        if (attempt > MAX_RETRIES) {
            // Send to DLQ if maximum retries reached
            producer.send(new ProducerRecord<>(DLQ_TOPIC_NAME, record.key(), record.value()));
            System.out.println("Event sent to DLQ: " + record.value());
        } else {
            // Send to retry topic with retry count
            String retryValue = record.value() + "|retry=" + attempt;
            producer.send(new ProducerRecord<>(TOPIC_NAME, record.key(), retryValue));
            System.out.println("Retrying event, attempt " + attempt + ": " + record.value());
        }
    }

    private int extractRetryCount(String value) {
        if (value.contains("retry=")) {
            return Integer.parseInt(value.split("retry=")[1]);
        }
        return 0;
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "inventory-service");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }

    private KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}
