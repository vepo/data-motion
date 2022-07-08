package io.vepo.datamotion.engine;

import io.vepo.datamotion.configuration.StreamerDefinition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Streamer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Streamer.class);
    private final StreamerDefinition definition;
    private CountDownLatch latch;
    private KafkaStreams streams;

    public Streamer(StreamerDefinition definition) {
        this.definition = definition;
    }

    public Streamer start() {
        logger.info("Starting Streamer! definition={}", definition);
        latch = new CountDownLatch(1);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, definition.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, definition.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        applyDefinition(builder.stream(definition.getInputTopic()))
                .to(definition.getOutputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime()
               .addShutdownHook(new Thread("streams-shutdown-hook") {
                   @Override
                   public void run() {
                       logger.info("Stopping Streamer! definition={}", definition);
                       streams.close();
                       latch.countDown();
                   }
               });
        streams.start();
        return this;
    }

    private KStream<String, String> applyDefinition(KStream<String, String> stream) {
        return stream;
    }

    public void join() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        StringWriter sw = new StringWriter();
        new Throwable("").printStackTrace(new PrintWriter(sw));
        logger.info("Stack trace! stack={}", sw.toString());
        logger.info("Request shutdown!");
        latch.countDown();
        streams.close();
    }
}
