package io.vepo.datamotion.engine;

import java.time.Duration;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vepo.datamotion.configuration.Deserializer;
import io.vepo.datamotion.configuration.Serializer;
import io.vepo.datamotion.configuration.StreamerDefinition;

class StreamerDockerTest extends AbstractStreamerDockerTest {

    private static final Logger logger = LoggerFactory.getLogger(StreamerDockerTest.class);

    @Test
    @DisplayName("Passthru String")
    void passthruStringTest() {
        logger.info("Is Kafka running? running={}", kafka.isRunning());
        createTopics("input", "output");
        try (Streamer<String, String> streamer = new Streamer<>(StreamerDefinition.<String, String>builder(Serializer.STRING, Deserializer.STRING)
                                                                .applicationId(APP_ID)
                                                                .inputTopic("input")
                                                                .outputTopic("output")
                                                                .bootstrapServers(kafka.getBootstrapServers())
                                                                .build());
             TestConsumer<String, String> consumer = start("output", StringDeserializer.class, StringDeserializer.class)) {
            streamer.start();
            sendMessage("input", "Hello World!");
            consumer.next((key, value) -> System.out.println("Key=" + key + " value=" + value), Duration.ofSeconds(160));
            logger.info("Test finished!");
        }
    }

    @Test
    @DisplayName("Passthru Long")
    void passthruLongTest() {
        logger.info("Is Kafka running? running={}", kafka.isRunning());
        createTopics("input", "output");
        try (Streamer<Long, Long> streamer = new Streamer<>(StreamerDefinition.<Long, Long>builder(Serializer.LONG, Deserializer.LONG)
                                                                .applicationId(APP_ID)
                                                                .inputTopic("input")
                                                                .outputTopic("output")
                                                                .bootstrapServers(kafka.getBootstrapServers())
                                                                .build());
            TestConsumer<String, Long> consumer = start("output", StringDeserializer.class, LongDeserializer.class)) {
            streamer.start();
            sendMessage("input", 55555L);
            consumer.next((key, value) -> System.out.println("Key=" + key + " value=" + value), Duration.ofSeconds(160));
            logger.info("Test finished!");
        }
    }
}