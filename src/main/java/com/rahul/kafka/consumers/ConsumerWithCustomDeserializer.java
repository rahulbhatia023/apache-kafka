package com.rahul.kafka.consumers;

import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.deserializers.CustomDeserializer;
import com.rahul.kafka.pojo.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithCustomDeserializer {
    public static void main(String[] args) {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());

        KafkaConsumer<String, Supplier> kafkaConsumer = new KafkaConsumer<>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(IKafkaConstants.TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, Supplier> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, Supplier> record : consumerRecords) {
                System.out.println("Supplier id= " + record.value().getID() + " Supplier Name = " + record.value().getName() + " Supplier Start Date = " + record.value().getStartDate().toString());
            }
        }
    }
}
