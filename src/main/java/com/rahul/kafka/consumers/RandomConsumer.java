package com.rahul.kafka.consumers;

import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.listeners.RebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class RandomConsumer {
    public static void main(String[] args) {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID);
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(false));

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);
        RebalanceListener rebalanceListener = new RebalanceListener(kafkaConsumer);
        kafkaConsumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME), rebalanceListener);

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    // Do some processing
                    rebalanceListener.addOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                }
            }
        } catch (Exception ex) {
            System.out.println("Exception");
            ex.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
