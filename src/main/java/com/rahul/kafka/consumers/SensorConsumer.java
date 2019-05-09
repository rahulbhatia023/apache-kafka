package com.rahul.kafka.consumers;

import com.rahul.kafka.IKafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SensorConsumer {
    public static void main(String[] args) {
        int rCount;

        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(false));

        /*
        We don't want Kafka to assign partitions to us.
        So, we create three partition objects, one each for the three partitions that we want to read, then we self-assign these three partitions.
         */
        TopicPartition p0 = new TopicPartition(IKafkaConstants.TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(IKafkaConstants.TOPIC_NAME, 1);
        TopicPartition p2 = new TopicPartition(IKafkaConstants.TOPIC_NAME, 2);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);
        kafkaConsumer.assign(Arrays.asList(p0, p1, p2));

        System.out.println("Current position p0=" + kafkaConsumer.position(p0)
                + " p1=" + kafkaConsumer.position(p1)
                + " p2=" + kafkaConsumer.position(p2));

        /*
        Set the offset position for three partitions
         */
        kafkaConsumer.seek(p0, getOffsetFromDB(p0));
        kafkaConsumer.seek(p1, getOffsetFromDB(p1));
        kafkaConsumer.seek(p2, getOffsetFromDB(p2));

        System.out.println("New positions p0=" + kafkaConsumer.position(p0)
                + " p1=" + kafkaConsumer.position(p1)
                + " p2=" + kafkaConsumer.position(p2));

        System.out.println("Start Fetching Now");

        try {
            do {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1000));
                System.out.println("Records polled:  " + consumerRecords.count());
                rCount = consumerRecords.count();
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
                    saveAndCommit(consumerRecord);
            } while (rCount > 0);
        } catch (Exception ex) {
            System.out.println("Exception in main.");
        } finally {
            kafkaConsumer.close();
        }
    }

    /*
    Reads offset positions from MySQL database
     */
    private static long getOffsetFromDB(TopicPartition topicPartition) {
        long offset = 0;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "pandey");

            String sql = "select offset from tss_offsets where topic_name='"
                    + topicPartition.topic() + "' and partition=" + topicPartition.partition();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next())
                offset = resultSet.getInt("offset");
            statement.close();
            connection.close();
        } catch (Exception e) {
            System.out.println("Exception in getOffsetFromDB");
        }
        return offset;
    }

    /*
    Save each record in the database and also update offsets
     */
    private static void saveAndCommit(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("Topic=" + consumerRecord.topic() + " Partition=" + consumerRecord.partition() + " Offset=" + consumerRecord.offset()
                + " Key=" + consumerRecord.key() + " Value=" + consumerRecord.value());
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "pandey");

            /*
            We set auto commit false, insert data, update offset, and finally, execute commit.
            That makes the method an atomic transaction and we can achieve exactly-once processing scenario.
             */
            connection.setAutoCommit(false);

            String insertSQL = "insert into tss_data values(?,?)";
            PreparedStatement psInsert = connection.prepareStatement(insertSQL);
            psInsert.setString(1, consumerRecord.key());
            psInsert.setString(2, consumerRecord.value());

            String updateSQL = "update tss_offsets set offset=? where topic_name=? and partition=?";
            PreparedStatement psUpdate = connection.prepareStatement(updateSQL);
            psUpdate.setLong(1, consumerRecord.offset() + 1);
            psUpdate.setString(2, consumerRecord.topic());
            psUpdate.setInt(3, consumerRecord.partition());

            psInsert.executeUpdate();
            psUpdate.executeUpdate();

            connection.commit();
            connection.close();
        } catch (Exception e) {
            System.out.println("Exception in saveAndCommit");
        }
    }
}
