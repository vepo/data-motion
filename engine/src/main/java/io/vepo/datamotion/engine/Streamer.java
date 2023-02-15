package io.vepo.datamotion.engine;

import java.io.Closeable;
import java.util.Collections;
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

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.vepo.datamotion.configuration.Deserializer;
import io.vepo.datamotion.configuration.Serializer;
import io.vepo.datamotion.configuration.StreamerDefinition;

public class Streamer<KI, VI, KO, VO> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Streamer.class);
    private final StreamerDefinition<KI, VI, KO, VO> definition;
    private CountDownLatch latch;
    private KafkaStreams streams;

    public Streamer(StreamerDefinition<KI, VI, KO, VO> definition) {
        this.definition = definition;
    }

    public static <KI, VI, KO, VO> Topology buildKafkaTopology(StreamerDefinition<KI, VI, KO, VO> definition) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<KI, VI> stream = builder.stream(definition.getInputTopic(), Consumed.<KI, VI>with(serde(definition.getKeyDeserializer()), serde(definition.getValueDeserializer())));
        KStream<KO, VO> defined = applyDefinition(stream);
        defined.to(definition.getOutputTopic(), Produced.<KO, VO>with(serde(definition.getKeySerializer()), serde(definition.getValueSerializer())));
        return builder.build();
    }

    private static <O> Serde<O> serde(Serializer serializer) {
        switch(serializer) {
            case STRING:
                return (Serde<O>) Serdes.String();
            case LONG:
                return (Serde<O>) Serdes.Long();
            case INT:
                return (Serde<O>) Serdes.Integer();
            case JSON:
                KafkaJsonSerializer jsonSerializer = new KafkaJsonSerializer();
                jsonSerializer.configure(Collections.emptyMap(), false);
                KafkaJsonDeserializer jsonDeserializer = new KafkaJsonDeserializer();
                jsonDeserializer.configure(Collections.emptyMap(), false);
                return (Serde<O>) Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
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
            case INT:
                return (Serde<I>) Serdes.Integer();
            case JSON:
                KafkaJsonSerializer jsonSerializer = new KafkaJsonSerializer();
                jsonSerializer.configure(Collections.emptyMap(), false);
                KafkaJsonDeserializer jsonDeserializer = new KafkaJsonDeserializer();
                jsonDeserializer.configure(Collections.emptyMap(), false);
                return (Serde<I>) Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
            default:
                throw new IllegalArgumentException("Not implemented yet! deserializer=" + deserializer);
        }
    }

    public static <KI, VI, KO, VO> Properties buildStreamProperties(StreamerDefinition<KI, VI, KO, VO> definition) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, definition.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, definition.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        return props;
    }


    public Streamer<KI, VI, KO, VO> start() {
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

    private static <KI, VI, KO, VO> KStream<KO, VO> applyDefinition(KStream<KI, VI> stream) {
        return (KStream<KO, VO>) stream.peek((key, value) ->{
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
