/*
Assume, we are collecting data from a bunch of sensors. All the sensors are sending data to a single topic.
I planned ten partitions for the topic. But I want three partitions dedicated for a specific sensor named TSS.
Remaining seven partitions are dedicated for rest of the sensors.
*/

package com.rahul.kafka.producers;

import com.rahul.kafka.IKafkaConstants;
import com.rahul.kafka.partitioners.CustomPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerWithCustomPartitioner {
    public static void main(String[] args) {
        String topicName = "SensorTopic";

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        /*
        The next property speed.sensor.name is not a Kafka configuration. It is a custom property.
        We are using it to supply the name of the sensor that requires special treatment.
        I don't want to hardcode the string TSS in my custom partitioner.
        Custom configuration is the method of passing values to your partitioner.
         */
        configProperties.put("speed.sensor.name", "TSS");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configProperties);

        for (int i = 0; i < 10; i++)
            kafkaProducer.send(new ProducerRecord<>(topicName, "SSP" + i, "500" + i));

        for (int i = 0; i < 10; i++)
            kafkaProducer.send(new ProducerRecord<>(topicName, "TSS", "500" + i));

        System.out.println("Message sent successfully");
        kafkaProducer.close();
    }
}

/*
On running this producer, output will be:

Key = SSP0 Partition = 4
Key = SSP1 Partition = 9
Key = SSP2 Partition = 5
Key = SSP3 Partition = 6
Key = SSP4 Partition = 3
Key = SSP5 Partition = 8
Key = SSP6 Partition = 3
Key = SSP7 Partition = 9
Key = SSP8 Partition = 6
Key = SSP9 Partition = 7
Key = TSS Partition = 1
Key = TSS Partition = 0
Key = TSS Partition = 0
Key = TSS Partition = 2
Key = TSS Partition = 0
Key = TSS Partition = 1
Key = TSS Partition = 0
Key = TSS Partition = 0
Key = TSS Partition = 1
Key = TSS Partition = 1

As you can see, both the messages with keys SSP3 and SSP8 landed on same partition number 6.
So we need to be careful while using key as a criteria for determining partition number.
 */
