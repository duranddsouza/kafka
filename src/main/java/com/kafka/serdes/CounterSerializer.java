package com.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.model.Counter;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CounterSerializer implements Serializer<Counter> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Counter counter) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(s).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;    }

    @Override
    public void close() {

    }
}
