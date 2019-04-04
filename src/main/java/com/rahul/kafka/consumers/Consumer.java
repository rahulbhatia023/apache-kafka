package com.rahul.kafka.consumers;

import com.rahul.kafka.IKafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        String topicName = "test"; // The topic 'test' is already created using create command

        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID);

        configProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);

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