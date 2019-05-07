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
        // The topic 'test' is already created using create command
        String topicName = "test";

        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /*
        You can specify your consumer group name as a value of this property.
        The group id property is not mandatory, so you can skip it.
        But you should know that when you are not part of any group that means you are starting an independent consumer.
        Your code will read all the data for the topic.
        Since you are not part of any group, there will be no sharing of work and your consumer need to read all data and process all of it alone.
         */
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);

        // Subscribing to a topic means you are informing Kafka broker that you want to read data for these topics.
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                /*
                The poll method will return some messages. You process them and again fetch for some more.
                The parameter to the poll method is a timeout.
                If there is no data to poll, you don't want to be hanging there.
                So this value specifies how quickly you want the poll method to return with or without data.
                 */
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