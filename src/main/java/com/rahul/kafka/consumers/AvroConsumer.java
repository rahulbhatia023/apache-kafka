package com.rahul.kafka.consumers;


import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.pojo.ClickRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AvroConsumer {
    public static void main(String[] args) {
        String topicName = "AvroClicks";
        String groupName = "RG";

        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        configProperties.put("schema.registry.url", "http://localhost:8081");
        configProperties.put("specific.avro.reader", "true");

        KafkaConsumer<String, ClickRecord> kafkaConsumer = new KafkaConsumer<>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, ClickRecord> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(100));
                for (ConsumerRecord<String, ClickRecord> consumerRecord : consumerRecords) {
                    System.out.println("Session id=" + consumerRecord.value().getSessionId()
                            + " Channel=" + consumerRecord.value().getChannel()
                            + " Ip=" + consumerRecord.value().getIp()
                            //+ " Referrer=" + consumerRecord.value().getReferrer()
                    );
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}