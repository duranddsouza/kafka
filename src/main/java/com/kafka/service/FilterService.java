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
import org.apache.kafka.streams.Topology;
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
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "raw");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.229:9092");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RecordDeserializer.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class.getName());


        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("raw", Consumed.with(Serdes.Long(),recordSerde))
                .filter((k,v) -> {
                    return pointInRectangle(v.getLattitude(), v.getLongitude());
                })
                .to("filtered", Produced.with(Serdes.Long(), recordSerde));

//        KTable<Long,Double> average = builder.stream("raw", Consumed.with(Serdes.Long(),recordSerde))
//                .filter((k,v) -> {
//                    return pointInRectangle(v.getLattitude(), v.getLongitude());
//                })
//                .groupByKey()
////                .windowedBy(TimeWindows.of(1 * 1 * 1000))
//                .aggregate(
//                new Initializer<Counter>() {
//                    @Override
//                    public Counter apply() {
//                        System.out.println("Initializing");
//                        return new Counter(0l, 0d);
//                    }
//                },
//                new Aggregator<Long, Record, Counter>() {
//                    @Override
//                    public Counter apply(final Long key, final Record value, final Counter counter) {
//                        ++counter.count;
//                        counter.value += value.getSpeed();
//                        System.out.println("applying");
//                        return counter;
//                    }
//                }, Materialized.<Long, Counter, KeyValueStore<Bytes, byte[]>>as("counts-store"))
//                .mapValues(new ValueMapper<Counter, Double>() {
//                            @Override
//                            public Double apply(Counter value) {
//                                return value.value / (double) value.count;
//                            }
//                        });
//
//        average.toStream().print(Printed.toSysOut());

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology,props);
        streams.start();

    }



    public boolean pointInRectangle(Double x, Double y) {
        return x < topLeftLatitude && x > bottomRightLatitude && y < bottomRightLongitude && y > topLeftLongitude;
    }
}
