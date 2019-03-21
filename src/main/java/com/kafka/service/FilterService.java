package com.kafka.service;


import com.kafka.serdes.RecordDeserializer;
import com.kafka.serdes.RecordSerde;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

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
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RecordDeserializer.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class.getName());

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("raw", Consumed.with(Serdes.Long(),recordSerde))
                .filter((k,v) -> {
                    return pointInRectangle(v.getLattitude(), v.getLongitude());
                }).to("filtered");

        KafkaStreams streams = new KafkaStreams(builder.build(),props);
        streams.start();
    }

    public boolean pointInRectangle(Double x, Double y) {
        return x < topLeftLatitude && x > bottomRightLatitude && y < bottomRightLongitude && y > topLeftLongitude;
    }
}
