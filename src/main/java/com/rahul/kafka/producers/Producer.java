package com.rahul.kafka.producers;

import com.rahul.kafka.IKafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configProperties);

        for (int i = 1; i <= 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, Integer.toString(i), Integer.toString(i));

            // Fire and Forget Approach: Refer important-notes.txt file
            kafkaProducer.send(producerRecord);
        }

        System.out.println("Message sent successfully");
        kafkaProducer.close();
    }
}