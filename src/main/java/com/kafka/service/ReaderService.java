package com.kafka.service;

import com.kafka.model.Record;
import com.kafka.serdes.RecordSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.*;

public class ReaderService {


    public void start() {

        try (BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/ais.csv")))) {
            // Skip header
            br.readLine();

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "filtered");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.229:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RecordSerializer.class.getName());

            KafkaProducer producer = new KafkaProducer(props);

            br.lines().forEach(r -> {
                Record record = new Record(r.split(","));
                ProducerRecord<Long, Record> p = new ProducerRecord<Long, Record>("raw", record.getMmsi(),record);
                producer.send(p);
            });

            producer.flush();
            producer.close(10000, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}


