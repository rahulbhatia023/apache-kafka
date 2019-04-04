package com.rahul.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {
        String topicName = "test"; // The topic 'test' is already created using create command

        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
                    System.out.println(consumerRecord.value());
            }
        } catch (Exception e) {
            System.out.println("Exception caught: " + e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}