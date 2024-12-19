package com.linuxacademy.ccdak.streams;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;


public class StatelessTransformationsMain {

    public static void main(String[] args) {
        // Set up the configuration.
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-transformations-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Since the input topic uses Strings for both key and value, set the default Serdes to String.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Get the source stream.
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source = builder.stream("count-2");
        //Implement streams logic.
        // branch stateless transformation
        // Split the stream into two streams, one containing all records where the key begins with "a" and ...
        KStream<String, String>[] branches = source.branch((k, v) -> k.startsWith("a"), (k, v) -> k.startsWith("b"), ((k, v) -> true));
        KStream<String, String> aKeyStream = branches[0];
        KStream<String, String> bKeyStream = branches[1];
        KStream<String, String> otherStream = branches[2];

        // filter value starting from "a"
        aKeyStream = aKeyStream.filter((key, value) -> value.startsWith("a"));

        // For the "a" stream, convert each record into two records, one with an uppercased value and one wit
        aKeyStream = aKeyStream.flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, value.toUpperCase()));
            result.add(KeyValue.pair(key, value.toLowerCase()));
            return result;
        });

        // Map
        aKeyStream = aKeyStream.map((key, value) -> KeyValue.pair(key.toUpperCase(), value));

        //Merge
        KStream<String,String> mergedStream = aKeyStream.merge(otherStream);

        //peek
        //Print each record to the console.
        mergedStream = mergedStream.peek((key, value) ->
                System.out.println("key=" + key + ", value=" + value));

        //output the final transform data to output topic
        // stream.to(outputTopicName);
        mergedStream.to("stateless-transformations-output-topic");
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        // Print the topology to the console.
        System.out.println(topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach a shutdown handler to catch control-c and terminate the application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

}
