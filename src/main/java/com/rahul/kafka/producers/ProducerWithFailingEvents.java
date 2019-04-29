package com.rahul.kafka.producers;

import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;

public class ProducerWithFailingEvents {
    public static void main(String[] args) throws IOException {
        KafkaProducer<String, String> invalidKafkaProducer = KafkaUtils.getKafkaProducer("kafka-producer-configs-invalid.properties");

        int count = 1;
        while (count <= 10) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME,
                    Integer.toString(count), Integer.toString(count));
            try {
                System.out.println("Sending Record: " + producerRecord.toString());

                // Synchronous Producer: Refer important-notes.txt file
                RecordMetadata recordMetadata = invalidKafkaProducer.send(producerRecord).get();

                System.out.println("Record is sent to partition number : " + recordMetadata.partition() + " and offset: " + recordMetadata.offset());
            } catch (Exception e) {
                System.out.println("Error in sending the message: " + producerRecord.value());
                System.out.println(e.getMessage());

                KafkaUtils.persistFailedRecords(producerRecord);
            }
            count++;
        }
        invalidKafkaProducer.close();

        KafkaProducer<String, String> validKafkaProducer = KafkaUtils.getKafkaProducer("kafka-producer-configs-valid.properties");
        KafkaUtils.publishFailedRecords(validKafkaProducer);
        validKafkaProducer.close();
    }
}