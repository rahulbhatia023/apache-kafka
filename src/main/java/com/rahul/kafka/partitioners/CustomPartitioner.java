package com.rahul.kafka.partitioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {
    private static String speedSensorName;

    /*
    The configure and the close methods are like initialization and clean-up methods.
    They will be called once at the time of instantiating your producer.
     */

    @Override
    public void configure(Map<String, ?> configs) {
        speedSensorName = configs.get("speed.sensor.name").toString();
    }

    /*
    The partition method is the place where all the action happens.
    The producer will call this method for each message and provide all the details with every call.
    The input to the method is the topic name, key, value and the cluster details.
    With all these input parameters, we have everything that is required to calculate a partition number.
    All that we need to do is to return an integer as a partition number.
    This method is the place where we implement our algorithm for partitioning.
     */
    @Override
    public int partition(String topicName, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /*
        Step-1: The first step is to determine the number of partitions and reserve 30% of it for TSS sensor.
         */
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topicName);
        int numPartitions = partitions.size();
        int numPartitionsForTSS = (int) Math.abs(numPartitions * 0.3);
        int partitionNumber;

        /*
        Step-2: If we don't get a message Key, throw an exception. We need the Key because the Key tells us the sensor name.
        Without knowing the sensor name, we can't decide that the message should go to one of the three reserved partitions
        or it should go to the the other bucket of seven partitions.
         */
        if ((keyBytes == null) || (!(key instanceof String)))
            throw new InvalidRecordException("All messages must have sensor name as key");

        /*
        Step-3: If the Key = TSS, then we hash the message value, divide it by 3 and take the mod as partition number.
        Using mod will make sure that we always get 0, 1 or 2
         */
        if (key.equals(speedSensorName))
            partitionNumber = Utils.toPositive(Utils.murmur2(valueBytes)) % numPartitionsForTSS;
        /*
        Step-4: If the Key != TSS then we divide it by 7 and again take the mod. The mod will be somewhere between 0 and 6.
        So, I am adding 3 to shift it by 3.
         */
        else
            partitionNumber = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numPartitionsForTSS) + numPartitionsForTSS;

        /*
        You might be wondering that in Step-3, I am hashing message value but in step 4, I am hashing message key.
        In step 3, every time Key will be TSS. So, hashing TSS will give me same number every time, and all the TSS messages
        will go to the same partition. But we want to distribute it in first three partitions. So, I am hashing the
        message value to get a different number every time.
        In step 4, I should be hashing the message value again. However, instead of hashing message value again, I am hashing the Key.
        That's because I wanted to show you that why you should be careful if you want to use a Key for achieving a specific partitioning.
        I wanted to demonstrate that different Keys can land up in the same partition.
         */

        System.out.println("Key = " + key + " Partition = " + partitionNumber);

        return partitionNumber;
    }

    @Override
    public void close() {

    }
}
