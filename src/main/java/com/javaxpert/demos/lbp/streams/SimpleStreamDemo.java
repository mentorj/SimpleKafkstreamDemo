package com.javaxpert.demos.lbp.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Simple demo for the Kafka Streams API
 * get messages from one topic (connect-test) , filter them then
 * push them to another topic(connect-test-out).
 */
public class SimpleStreamDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            props.load(SimpleStreamDemo.class.getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("connect-test", Consumed.with(Serdes.String(), Serdes.String()))
                .filter((name,msg) -> msg.toLowerCase(Locale.ROOT).trim().contains("test"))

                .to("connect-test-out",
                        Produced.with(Serdes.String(),Serdes.String())
                );
        final Topology topology = builder.build();
        System.out.println(topology.describe().toString());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
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
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}
