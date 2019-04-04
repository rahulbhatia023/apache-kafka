package com.rahul.kafka.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomSerializer implements Serializer {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String topicName, Object data) {
        byte[] serializedValue = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            serializedValue = objectMapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return serializedValue;
    }

    @Override
    public void close() {

    }
}
