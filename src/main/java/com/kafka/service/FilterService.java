package com.kafka.service;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.kafka.model.Counter;
import com.kafka.model.Record;
import com.kafka.serdes.CounterSerde;
import com.kafka.serdes.RecordDeserializer;
import com.kafka.serdes.RecordSerde;

import com.spotify.docker.client.shaded.com.fasterxml.jackson.databind.JsonDeserializer;
import com.spotify.docker.client.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.spotify.docker.client.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.spotify.docker.client.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class FilterService {

    private Double topLeftLatitude;
    private Double topLeftLongitude;
    private Double bottomRightLatitude;
    private Double bottomRightLongitude;

    public FilterService(Double topLeftLatitude, Double topLeftLongitude, Double bottomRightLatitude, Double bottomRightLongitude) {
        this.topLeftLatitude = topLeftLatitude;
        this.topLeftLongitude = topLeftLongitude;
        this.bottomRightLatitude = bottomRightLatitude;
        this.bottomRightLongitude = bottomRightLongitude;
    }



    public void start() {
        RecordSerde recordSerde = new RecordSerde();
        CounterSerde counterSerde = new CounterSerde();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "raw");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RecordDeserializer.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class.getName());


        final StreamsBuilder builder = new StreamsBuilder();
//        builder.stream("raw", Consumed.with(Serdes.String(),recordSerde))
//                .filter((k,v) -> {
//                    System.out.println(v.getMmsi());
//                    return pointInRectangle(v.getLattitude(), v.getLongitude());
//                }).to("filtered");

        KGroupedStream<Long, Record> groupedStream = builder.stream("raw", Consumed.with(Serdes.Long(),recordSerde))
                .filter((k,v) -> {
                    System.out.println(v.getMmsi());
                    return pointInRectangle(v.getLattitude(), v.getLongitude());
                })
                .groupByKey();


        final KTable<Long,Counter> countAndSum  = groupedStream.aggregate(
                new Initializer<Counter>() {
                    @Override
                    public Counter apply() {
                        return new Counter(0l, 0d);
                    }
                },
                new Aggregator<Long, Record, Counter>() {
                    @Override
                    public Counter apply(final Long key, final Record value, final Counter counter) {
                        ++counter.count;
                        counter.value += value.getSpeed();
                        return counter;
                    }
                }, Materialized.<Long, Counter, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        final KTable<Long,Double> average =  countAndSum.mapValues(new ValueMapper<Counter, Double>() {
                            @Override
                            public Double apply(Counter value) {
                                return value.value / (double) value.count;
                            }
                        });

        average.toStream().print(Printed.toSysOut());
        KafkaStreams streams = new KafkaStreams(builder.build(),props);
        streams.start();

    }



    public boolean pointInRectangle(Double x, Double y) {
        return x < topLeftLatitude && x > bottomRightLatitude && y < bottomRightLongitude && y > topLeftLongitude;
    }
}
