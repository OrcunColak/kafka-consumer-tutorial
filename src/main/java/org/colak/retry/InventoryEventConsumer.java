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

class InventoryEventConsumer {
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    private static final String TOPIC_NAME = "order_events";

    public static void main(String[] args) {
        InventoryEventConsumer inventoryEventConsumer = new InventoryEventConsumer();
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
            for (ConsumerRecord<String, String> consumerRecord : records) {
                try {
                    processEvent(consumerRecord);
                } catch (Exception e) {
                    handleRetry(consumerRecord); // Initial retry count
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

    private void handleRetry(ConsumerRecord<String, String> record) {
        // Send to retry topic with retry count
        String retryValue = record.value() + "|retry=" + 1;
        producer.send(new ProducerRecord<>("order_events_retry", record.key(), retryValue));
        System.out.println("Retrying event, attempt " + 1 + ": " + record.value());
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
