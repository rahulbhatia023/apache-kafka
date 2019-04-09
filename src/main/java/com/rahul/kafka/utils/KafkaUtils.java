package com.rahul.kafka.utils;

import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.producers.ProducerWithFailingEvents;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

public class KafkaUtils {
    public static KafkaProducer<String, String> getKafkaProducer(String configPropertiesFileName) throws IOException {
        Properties configProperties = new Properties();
        configProperties.load(ProducerWithFailingEvents.class.getClassLoader().getResourceAsStream(configPropertiesFileName));

        return new KafkaProducer<>(configProperties);
    }

    public static void persistFailedRecords(ProducerRecord<String, String> producerRecord) {
        String key = producerRecord.key();
        String value = producerRecord.value();

        Jedis jedis = new Jedis("localhost");
        jedis.set(key, value);

        System.out.println("Record with KEY: " + key + " and VALUE: " + value + " persisted in REDIS");
    }

    public static void publishFailedRecords(KafkaProducer<String, String> validKafkaProducer) {
        Jedis jedis = new Jedis("localhost");
        Set<String> keySet = jedis.keys("*");

        for (String key : keySet) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME,
                    key, jedis.get(key));
            System.out.println("Publishing failed record: " + producerRecord);
            validKafkaProducer.send(producerRecord);

            jedis.del(key);
        }
    }
}