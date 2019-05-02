package com.rahul.kafka.serializers;

import com.rahul.kafka.pojo.Supplier;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomSerializer implements Serializer<Supplier> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, Supplier supplier) {
        int sizeOfName;
        int sizeOfDate;
        byte[] serializedName;
        byte[] serializedDate;

        try {
            if (supplier == null)
                return null;

            // We simply convert supplier name and supplier start date into UTF8 bytes
            serializedName = supplier.getName().getBytes(encoding);
            sizeOfName = serializedName.length;
            serializedDate = supplier.getStartDate().toString().getBytes(encoding);
            sizeOfDate = serializedDate.length;

            /*
            We allocate a byte buffer and encode everything into the byte buffer.
            Since we will need to know the length of supplier name and supplier date strings at the time of deserialization,
            we also encode their sizes into the byte buffer
             */
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + sizeOfName + 4 + sizeOfDate);
            byteBuffer.putInt(supplier.getID());
            byteBuffer.putInt(sizeOfName);
            byteBuffer.put(serializedName);
            byteBuffer.putInt(sizeOfDate);
            byteBuffer.put(serializedDate);

            return byteBuffer.array();

        } catch (Exception e) {
            throw new SerializationException("Error when serializing Supplier to byte[]");
        }
    }

    @Override
    public void close() {
    }
}
