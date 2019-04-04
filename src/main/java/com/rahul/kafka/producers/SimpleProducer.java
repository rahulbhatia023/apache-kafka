package com.rahul.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        String topicName = "test"; // The topic 'test' is already created using create command

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configProperties);

        for (int i = 1; i <= 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, Integer.toString(i), Integer.toString(i));
            kafkaProducer.send(producerRecord);
        }

        System.out.println("Message sent successfully");
        kafkaProducer.close();
    }
}
