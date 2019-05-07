/*
In this example, we will use asynchronous commit. But in the case of an error, we want to make sure that we commit before we close and exit.
So, we will use synchronous commit before we close our consumer.
 */

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

public class ConsumersWIthCommitOffset {
    public static void main(String[] args) {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(false));

        KafkaConsumer<String, Supplier> kafkaConsumer = new KafkaConsumer<>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(IKafkaConstants.TOPIC_NAME));

        try {
            while (true) {
                ConsumerRecords<String, Supplier> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(100));
                for (ConsumerRecord<String, Supplier> consumerRecord : consumerRecords) {
                    System.out.println("Supplier Id= " + consumerRecord.value().getID() +
                            " Supplier Name = " + consumerRecord.value().getName() +
                            " Supplier Start Date = " + consumerRecord.value().getStartDate().toString());
                }
                kafkaConsumer.commitAsync();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            kafkaConsumer.commitSync();
            kafkaConsumer.close();
        }
    }
}
