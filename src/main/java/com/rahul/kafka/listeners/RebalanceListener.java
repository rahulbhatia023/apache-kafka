/*
Let's first understand the responsibilities of the listener. We want it to take care of two things.

Maintain a list of offsets that are processed and ready to be committed.
Commit the offsets when partitions are going away.
So, we want to maintain our personal list of offsets instead of relying on the current offsets that are managed by Kafka.
This list will give us a complete freedom on what we want to commit.
 */

package com.rahul.kafka.listeners;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RebalanceListener implements ConsumerRebalanceListener {
    private KafkaConsumer kafkaConsumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

    public RebalanceListener(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public void addOffset(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    /*
    The onPartitionsAssigned method will print the list of partitions that are assigned.
     */
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Assigned ....");
        for (TopicPartition partition : partitions)
            System.out.println(partition.partition() + ",");
    }

    /*
    The onPartitionsRevoked method will also print the list of partitions that are going away. Then, it will commit and reset the list.
     */
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Revoked ....");
        for (TopicPartition partition : partitions)
            System.out.println(partition.partition() + ",");

        System.out.println("Following Partitions committed ....");
        for (TopicPartition tp : currentOffsets.keySet())
            System.out.println(tp.partition());

        kafkaConsumer.commitSync(currentOffsets);
        currentOffsets.clear();
    }
}
