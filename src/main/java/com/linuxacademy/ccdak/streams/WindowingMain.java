package com.linuxacademy.ccdak.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

public class WindowingMain {

    public static void main(String[] args) {
        // Set up the configuration.
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowing-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Since the input topic uses Strings for both key and value, set the default Serdes to String.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Get the source stream.
        final StreamsBuilder builder = new StreamsBuilder();

        //Implement streams logic.
        KStream<String, String> source = builder.stream("windowing-input-topic");
        KGroupedStream<String, String> groupedStream = source.groupByKey();

        // Apply windowing to the stream with tumbling time windows of 10 seconds.
        TimeWindowedKStream<String, String> windowedStream = groupedStream.
                windowedBy(TimeWindows.of(Duration.ofSeconds(10)));
        // windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(10)));
        // windowedBy(TimeWindows.of(Duration.ofSeconds( 10)) how long is your window,
        // single window will open till 10sec;
        // advanceBy(Duration.ofSeconds(10) how ofter you open your window every 10 seconds
        // here if you want to create Hoping time window you just need to increase time into .advanceBy(12)
        // so, here 2 sec gap
        // if  you remove this option then also you got tumbling time window without any gap and overlap

        // Combine the values of all records with the same key into a string separated by spaces, using 10-seconds window
        KTable<Windowed<String>, String> reducedTable = windowedStream.reduce((aggVale, newValue) -> aggVale + " " + newValue);
        reducedTable.toStream()
                .to("windowing-output-topic", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

        // end of implement
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
