package com.rahul.kafka.producers;

import com.rahul.kafka.IKafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

public class RandomProducer {
    public static void main(String[] args) {
        String message;

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configProperties);

        Random random = new Random();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2016, 1, 1);

        /*
        The topic is already created and it has two partitions.
        If you look at those two send method calls, you will notice that we are sending the first message to partition zero
        and then next message to partition one.
        I am making these calls in an infinite loop, so the producer will keep sending data to both the partitions.
        This flow of continuous messages to both the partitions will help us simulate a rebalance and understand the behaviour of consumers.
         */
        try {
            while (true) {
                for (int i = 0; i < 100; i++) {
                    message = calendar.get(Calendar.YEAR) + "-" +
                            calendar.get(Calendar.MONTH) + "-" +
                            calendar.get(Calendar.DATE) + "," +
                            random.nextInt(1000);

                    producer.send(new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, 0, "Key", message)).get();

                    message = calendar.get(Calendar.YEAR) + "-" +
                            calendar.get(Calendar.MONTH) + "-" +
                            calendar.get(Calendar.DATE) + "," +
                            random.nextInt(1000);

                    producer.send(new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, 1, "Key", message)).get();
                }
                calendar.add(Calendar.DATE, 1);
                System.out.println("Data Sent for " +
                        calendar.get(Calendar.YEAR) + "-" +
                        calendar.get(Calendar.MONTH) + "-" +
                        calendar.get(Calendar.DATE));
            }
        } catch (Exception ex) {
            System.out.println("Interrupted");
        } finally {
            producer.close();
        }
    }
}
