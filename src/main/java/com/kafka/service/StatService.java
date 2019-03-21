package com.kafka.service;

import com.kafka.model.Counter;
import com.kafka.model.Record;
import com.kafka.serdes.CounterSerde;
import com.kafka.serdes.RecordDeserializer;
import com.kafka.serdes.RecordSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;

import static org.apache.kafka.streams.kstream.Materialized.as;

public class StatService {

    public void start() {
        RecordSerde recordSerde = new RecordSerde();
        CounterSerde counterSerde = new CounterSerde();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stats");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.229:9092");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RecordDeserializer.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class.getName());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, Record> stream = builder.stream("filtered", Consumed.with(Serdes.Long(),recordSerde));
        KTable<Windowed<Long>, String> speed = stream.mapValues(new ValueMapper<Record, String>() {
            @Override
            public String apply(Record record) {
                return record.toString();
            }
        }).groupByKey().windowedBy(TimeWindows.of(1000))
                .reduce((a,v) -> a + v);
//        KTable<Windowed<Long>, Double> speed =stream.mapValues(new ValueMapper<Record, Double>() {
//            @Override
//            public Double apply(Record record) {
//                return record.getSpeed();
//            }
//        }).groupByKey().windowedBy(TimeWindows.of(1000)).reduce((aggValue, newValue) -> aggValue + newValue);

        KTable<Windowed<Long>, Long> count = stream.groupByKey().windowedBy(TimeWindows.of(1000)).count();

        speed.toStream().foreach((k,v) -> System.out.println(k + ":" + v));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
