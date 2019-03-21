package com.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.model.Record;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class RecordSerializer implements Serializer<Record> {


    @Override
    public void configure(Map map, boolean b) {

    }

    @Override public byte[] serialize(String arg0, Record arg1) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(arg1).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
