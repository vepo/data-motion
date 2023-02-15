package io.vepo.datamotion.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Objects;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vepo.datamotion.configuration.Deserializer;
import io.vepo.datamotion.configuration.Serializer;
import io.vepo.datamotion.configuration.StreamerDefinition;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;

@DisplayName("Docker")
class StreamerDockerTest extends AbstractStreamerDockerTest {

    private static final Logger logger = LoggerFactory.getLogger(StreamerDockerTest.class);

    public static class KeyPojo {
        private String id;

        public KeyPojo() {
        }

        public KeyPojo(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            } else {
                KeyPojo that = (KeyPojo) o;
                return Objects.equals(id, that.id);
            }
        }

        @Override
        public String toString() {
            return String.format("KeyPojo [id=%s]",  id);
        }
    }

    public static class ValuePojo {
        private String x;
        private Integer y;

        public ValuePojo() {
        }

        public ValuePojo(String x, Integer y) {
            this.x = x;
            this.y = y;
        }

        public String getX() {
            return x;
        }

        public void setX(String x) {
            this.x = x;
        }

        public Integer getY() {
            return y;
        }

        public void setY(Integer y) {
            this.y = y;
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, y);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            } else {
                ValuePojo that = (ValuePojo) o;
                return Objects.equals(x, that.x) &&
                        Objects.equals(y, that.y);
            }
        }

        @Override
        public String toString() {
            return String.format("ValuePojo [x=%s, y=%s]",  x, y);
        }
    }

    @Nested
    @DisplayName("Passthru")
    class Passthru {
        @Test
        @DisplayName("String")
        void stringTest() {
            logger.info("Is Kafka running? running={}", kafka.isRunning());
            createTopics("input", "output");
            try (Streamer<String, String, String, String> streamer = new Streamer<>(StreamerDefinition.<String, String, String, String>builder()
                                                                                                      .applicationId(APP_ID)
                                                                                                      .keySerializer(Serializer.STRING)
                                                                                                      .valueSerializer(Serializer.STRING)
                                                                                                      .keyDeserializer(Deserializer.STRING)
                                                                                                      .valueDeserializer(Deserializer.STRING)
                                                                                                      .inputTopic("input")
                                                                                                      .outputTopic("output")
                                                                                                      .bootstrapServers(kafka.getBootstrapServers())
                                                                                                      .build());
                 TestConsumer<String, String> consumer = start("output", StringDeserializer.class, StringDeserializer.class)) {
                streamer.start();
                sendMessage("input", "key-1", "Hello World!");
                consumer.next((key, value) -> {
                    assertEquals("key-1", key);
                    assertEquals("Hello World!", value);
                }, Duration.ofSeconds(160));
                logger.info("Test finished!");
            }
        }

        @Test
        @DisplayName("Long")
        void longTest() {
            logger.info("Is Kafka running? running={}", kafka.isRunning());
            createTopics("input", "output");
            try (Streamer<Long, Long, Long, Long> streamer = new Streamer<>(StreamerDefinition.<Long, Long, Long, Long>builder()
                                                                                              .applicationId(APP_ID)
                                                                                              .keySerializer(Serializer.LONG)
                                                                                              .valueSerializer(Serializer.LONG)
                                                                                              .keyDeserializer(Deserializer.LONG)
                                                                                              .valueDeserializer(Deserializer.LONG)
                                                                                              .inputTopic("input")
                                                                                              .outputTopic("output")
                                                                                              .bootstrapServers(kafka.getBootstrapServers())
                                                                                              .build());
                 TestConsumer<Long, Long> consumer = start("output", LongDeserializer.class, LongDeserializer.class)) {
                streamer.start();
                sendMessage("input", 8888L, 55555L);
                consumer.next((key, value) -> {
                    assertEquals(8888L, key);
                    assertEquals(55555L, value);
                }, Duration.ofSeconds(160));
                logger.info("Test finished!");
            }
        }

        @Test
        @DisplayName("Int")
        void intTest() {
            logger.info("Is Kafka running? running={}", kafka.isRunning());
            createTopics("input", "output");
            try (Streamer<Integer, Integer, Integer, Integer> streamer = new Streamer<>(StreamerDefinition.<Integer, Integer, Integer, Integer>builder()
                                                                                                          .applicationId(APP_ID)
                                                                                                          .keySerializer(Serializer.INT)
                                                                                                          .valueSerializer(Serializer.INT)
                                                                                                          .keyDeserializer(Deserializer.INT)
                                                                                                          .valueDeserializer(Deserializer.INT)
                                                                                                          .inputTopic("input")
                                                                                                          .outputTopic("output")
                                                                                                          .bootstrapServers(kafka.getBootstrapServers())
                                                                                                          .build());
                 TestConsumer<Integer, Integer> consumer = start("output", IntegerDeserializer.class, IntegerDeserializer.class)) {
                streamer.start();
                sendMessage("input", 4444, 555);
                consumer.next((key, value) -> {
                    assertEquals(4444, key);
                    assertEquals(555, value);
                }, Duration.ofSeconds(160));
                logger.info("Test finished!");
            }
        }

        @Test
        @DisplayName("JSON")
        void jsonTest() {
            logger.info("Is Kafka running? running={}", kafka.isRunning());
            createTopics("input", "output");
            try (Streamer<KeyPojo, ValuePojo, KeyPojo, ValuePojo> streamer = new Streamer<>(StreamerDefinition.<KeyPojo, ValuePojo, KeyPojo, ValuePojo>builder()
                                                                                                              .applicationId(APP_ID)
                                                                                                              .keySerializer(Serializer.JSON)
                                                                                                              .valueSerializer(Serializer.JSON)
                                                                                                              .keyDeserializer(Deserializer.JSON)
                                                                                                              .valueDeserializer(Deserializer.JSON)
                                                                                                              .inputTopic("input")
                                                                                                              .outputTopic("output")
                                                                                                              .bootstrapServers(kafka.getBootstrapServers())
                                                                                                              .build());
                  TestConsumer<String, String> consumer = start("output", StringDeserializer.class, StringDeserializer.class)) {
                streamer.start();
                sendMessage("input", new KeyPojo("XXXX"), KafkaJsonSerializer.class , new ValuePojo("YYY", 33), KafkaJsonSerializer.class);
                consumer.next((key, value) -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        assertEquals(new KeyPojo("XXXX"), mapper.readValue(key, KeyPojo.class));
                        assertEquals(new ValuePojo("YYY", 33), mapper.readValue(value, ValuePojo.class));
                    } catch (Exception ex) {
                        fail(ex);
                    }
                }, Duration.ofSeconds(160));
                logger.info("Test finished!");
            }
        }
    }
}