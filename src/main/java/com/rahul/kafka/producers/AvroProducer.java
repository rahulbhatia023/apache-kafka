package com.rahul.kafka.producers;

import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.pojo.ClickRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AvroProducer {
    public static void main(String[] args) {
        String topicName = "AvroClicks";
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configProperties.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, ClickRecord> kafkaProducer = new KafkaProducer<>(configProperties);

        ClickRecord clickRecord = new ClickRecord();
        clickRecord.setSessionId("10001");
        clickRecord.setChannel("HomePage");
        clickRecord.setIp("192.168.0.1");

        try {
            kafkaProducer.send(new ProducerRecord<>(topicName, clickRecord.getSessionId().toString(), clickRecord)).get();
            System.out.println("Complete");
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        } finally {
            kafkaProducer.close();
        }
    }
}