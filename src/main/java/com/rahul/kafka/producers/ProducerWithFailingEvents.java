package com.rahul.kafka.producers;

import com.rahul.kafka.IKafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.Jedis;

import java.util.Properties;

public class ProducerWithFailingEvents {
    public static void main(String[] args) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configProperties);

        int count = 0;
        while (true) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, Integer.toString(count), Integer.toString(count));
            try {
                System.out.println("Sending Record: " + producerRecord.toString());
                kafkaProducer.send(producerRecord).get();
            } catch (Exception e) {
                System.out.println("Error in sending the message: " + producerRecord.value());
                System.out.println(e.getMessage());

                persistRecordInRedis(producerRecord);
            }
            count++;
        }
    }

    private static void persistRecordInRedis(ProducerRecord<String, String> producerRecord) {
        String key = producerRecord.key();
        String value = producerRecord.value();

        Jedis jedis = new Jedis("localhost");
        jedis.set(key, value);

        System.out.println("Record with KEY: " + key + " and VALUE: " + value + " persisted in REDIS");
    }
}