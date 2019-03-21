package com.kafka.serdes;

import com.kafka.model.Record;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class RecordSerde implements Serde<Record> {
    private RecordSerializer recordSerializer = new RecordSerializer();
    private RecordDeserializer recordDeserializer = new RecordDeserializer();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {
        recordSerializer.close();
        recordDeserializer.close();
    }

    @Override
    public Serializer<Record> serializer() {
        return recordSerializer;
    }

    @Override
    public Deserializer<Record> deserializer() {
        return recordDeserializer;
    }
}
