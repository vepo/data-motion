package io.vepo.datamotion.engine;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vepo.datamotion.configuration.Deserializer;
import io.vepo.datamotion.configuration.Serializer;
import io.vepo.datamotion.configuration.StreamerDefinition;

public class Streamer<I, O> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Streamer.class);
    private final StreamerDefinition<I, O> definition;
    private CountDownLatch latch;
    private KafkaStreams streams;

    public Streamer(StreamerDefinition<I, O> definition) {
        this.definition = definition;
    }

    public static <I, O> Topology buildKafkaTopology(StreamerDefinition<I, O> definition) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, I> stream = builder.stream(definition.getInputTopic(), Consumed.<String, I>with(Serdes.String(), serde(definition.getDeserializer())));
        KStream<String, O> defined = applyDefinition(stream);
        defined.to(definition.getOutputTopic(), Produced.<String, O>with(Serdes.String(), serde(definition.getSerializer())));
        return builder.build();
    }

    private static <O> Serde<O> serde(Serializer serializer) {
        switch(serializer) {
            case STRING:
                return (Serde<O>) Serdes.String();
            case LONG:
                return (Serde<O>) Serdes.Long();
            default:
                throw new IllegalArgumentException("Not implemented yet! serializer=" + serializer);
        }
    }

    private static <I> Serde<I> serde(Deserializer deserializer) {
        switch(deserializer) {
            case STRING:
                return (Serde<I>) Serdes.String();
            case LONG:
                return (Serde<I>) Serdes.Long();
            default:
                throw new IllegalArgumentException("Not implemented yet! deserializer=" + deserializer);
        }
    }

    public static <I, O> Properties buildStreamProperties(StreamerDefinition<I, O> definition) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, definition.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, definition.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        return props;
    }


    public Streamer<I, O> start() {
        logger.info("Starting Streamer! definition={}", definition);
        latch = new CountDownLatch(1);
        streams = new KafkaStreams(buildKafkaTopology(definition), buildStreamProperties(definition));

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

        while (streams.state() != State.RUNNING) {
            try {
                Thread.sleep(500);
                if (streams.state() == State.ERROR) {
                    return this;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        logger.info("Streamer started! definition={}", definition);
        return this;
    }

    private static <I, O> KStream<String, O> applyDefinition(KStream<String, I> stream) {
        return (KStream<String, O>) stream.peek((key, value) ->{
            logger.info("Message passed! key={} value={}", key, value);
        });
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
        logger.info("Request shutdown!");
        latch.countDown();
        streams.close();
    }
}
