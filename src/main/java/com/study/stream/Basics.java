package com.study.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class Basics {
    final static String INPUT_TOPIC = "stream-basic-input-1";
    final static String OUTPUT_TOPIC = "stream-basic-output-1";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put("application.id", "basic-stream");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("cache.max.bytes.buffering", 0);
        // Since the input topic uses Strings for both key and value, set the default Serdes to String.
        props.put("default.key.serde", Serdes.String().getClass().getName());
        props.put("default.value.serde", Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        kStream.peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));
        KStream<String, String> stringStringKStream = kStream.mapValues(s -> s.toUpperCase());
        stringStringKStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        System.out.println("description of the topology is:::");
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);
        System.out.println("call Stream state through    streams.state();");
        streams.state();
        System.out.println("Start Stream through    streams.start();");
        streams.start();

    }
}
