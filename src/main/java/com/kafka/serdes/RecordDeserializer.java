package com.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.model.Record;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class RecordDeserializer implements Deserializer <Record> {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Record deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        Record record = null;
        try {
            record = mapper.readValue(arg1, Record.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return record;
    }

    @Override
    public void close() {

    }
}
