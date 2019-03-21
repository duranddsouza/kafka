package com.kafka.serdes;

import com.kafka.model.Counter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CounterSerde implements Serde<Counter> {

    private CounterDeserializer counterDeserializer = new CounterDeserializer();
    private CounterSerializer counterSerializer = new CounterSerializer();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {
        counterSerializer.close();
        counterDeserializer.close();
    }

    @Override
    public Serializer<Counter> serializer() {
        return counterSerializer;
    }

    @Override
    public Deserializer<Counter> deserializer() {
        return counterDeserializer;
    }
}
