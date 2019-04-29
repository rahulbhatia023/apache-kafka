package com.rahul.kafka.producers;

import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;

public class AsynchronousProducer {
    public static void main(String[] args) throws IOException {
        KafkaProducer<String, String> invalidKafkaProducer = KafkaUtils.getKafkaProducer("kafka-producer-configs-invalid.properties");

        for (int i = 1; i <= 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, Integer.toString(i), Integer.toString(i));
            invalidKafkaProducer.send(producerRecord, new MyProducerCallback());
        }
        invalidKafkaProducer.close();
    }
}

class MyProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null)
            System.out.println("AsynchronousProducer failed with an exception");
        else
            System.out.println("AsynchronousProducer call Success");
    }
}
