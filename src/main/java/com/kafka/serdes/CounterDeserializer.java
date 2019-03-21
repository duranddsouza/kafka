package com.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.model.Counter;
import com.kafka.model.Record;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CounterDeserializer implements Deserializer<Counter> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Counter deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Counter counter = null;
        try {
            counter = mapper.readValue(s, Counter.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return counter;
    }

    @Override
    public void close() {

    }
}
