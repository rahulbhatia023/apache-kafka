package com.rahul.kafka.producers;

import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.pojo.Supplier;
import com.rahul.kafka.serializers.CustomSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithCustomSerializer {
    public static void main(String[] args) throws IOException, ParseException, ExecutionException, InterruptedException {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class.getName());

        KafkaProducer<String, Supplier> kafkaProducer = new KafkaProducer<>(configProperties);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Supplier supplier1 = new Supplier(101, "Xyz Pvt Ltd.", dateFormat.parse("2016-04-01"));
        Supplier supplier2 = new Supplier(102, "Abc Pvt Ltd.", dateFormat.parse("2012-01-01"));

        kafkaProducer.send(new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, "SUP", supplier1)).get();
        kafkaProducer.send(new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, "SUP", supplier2)).get();

        kafkaProducer.close();
    }
}