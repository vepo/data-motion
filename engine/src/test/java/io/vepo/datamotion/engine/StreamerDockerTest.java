package io.vepo.datamotion.engine;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.vepo.datamotion.configuration.Deserializer;
import io.vepo.datamotion.configuration.Serializer;
import io.vepo.datamotion.configuration.StreamerDefinition;
import io.vepo.datamotion.engine.serdes.OfflineAvroSerde;
import io.vepo.datamotion.test.pojos.avro.Document;
import io.vepo.datamotion.test.pojos.avro.Id;
import io.vepo.datamotion.test.pojos.protobuf.Key;
import io.vepo.datamotion.test.pojos.protobuf.Person;

@Tag("docker")
@DisplayName("Docker")
class StreamerDockerTest extends AbstractStreamerDockerTest {

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
            createTopics("input", "output");
            try (Streamer<String, String, String, String> streamer = new Streamer<>(StreamerDefinition.<String, String, String, String>builder(String.class, String.class, String.class, String.class)
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
            }
        }

        @Test
        @DisplayName("Long")
        void longTest() {
            createTopics("input", "output");
            try (Streamer<Long, Long, Long, Long> streamer = new Streamer<>(StreamerDefinition.<Long, Long, Long, Long>builder(Long.class, Long.class, Long.class, Long.class)
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
            }
        }

        @Test
        @DisplayName("Int")
        void intTest() {
            createTopics("input", "output");
            try (Streamer<Integer, Integer, Integer, Integer> streamer = new Streamer<>(StreamerDefinition.<Integer, Integer, Integer, Integer>builder(Integer.class, Integer.class, Integer.class, Integer.class)
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
            }
        }

        @Test
        @DisplayName("JSON")
        void jsonTest() {
            createTopics("input", "output");
            try (Streamer<KeyPojo, ValuePojo, KeyPojo, ValuePojo> streamer = new Streamer<>(StreamerDefinition.<KeyPojo, ValuePojo, KeyPojo, ValuePojo>builder(KeyPojo.class, ValuePojo.class, KeyPojo.class, ValuePojo.class)
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
            }
        }

        @Test
        @DisplayName("AVRO - Dynamic Schema")
        void dynamicAvroTest() {
            createTopics("input", "output");
            try (Streamer<KeyPojo, ValuePojo, KeyPojo, ValuePojo> streamer = new Streamer<>(StreamerDefinition.<KeyPojo, ValuePojo, KeyPojo, ValuePojo>builder(KeyPojo.class, ValuePojo.class, KeyPojo.class, ValuePojo.class)
                                                                                                              .applicationId(APP_ID)
                                                                                                              .keySerializer(Serializer.AVRO)
                                                                                                              .valueSerializer(Serializer.AVRO)
                                                                                                              .keyDeserializer(Deserializer.AVRO)
                                                                                                              .valueDeserializer(Deserializer.AVRO)
                                                                                                              .inputTopic("input")
                                                                                                              .outputTopic("output")
                                                                                                              .bootstrapServers(kafka.getBootstrapServers())
                                                                                                              .build());
                  TestConsumer<byte[], byte[]> consumer = start("output", ByteArrayDeserializer.class, ByteArrayDeserializer.class)) {
                
                Serde<KeyPojo> keySerde = new OfflineAvroSerde<>(KeyPojo.class);
                Serde<ValuePojo> valueSerde = new OfflineAvroSerde<>(ValuePojo.class);
                streamer.start();
                sendMessage("input", keySerde.serializer().serialize(null, new KeyPojo("XXXX")), ByteArraySerializer.class , valueSerde.serializer().serialize(null,  new ValuePojo("YYY", 33)), ByteArraySerializer.class);
                consumer.next((key, value) -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        assertEquals(new KeyPojo("XXXX"), keySerde.deserializer().deserialize(null, key));
                        assertEquals(new ValuePojo("YYY", 33), valueSerde.deserializer().deserialize(null, value));
                    } catch (Exception ex) {
                        fail(ex);
                    }
                }, Duration.ofSeconds(160));
            }
        }

        @Test
        @DisplayName("AVRO - Dynamic Schema")
        void specificAvroTest() {
            createTopics("input", "output");
            try (Streamer<Id, Document, Id, Document> streamer = new Streamer<>(StreamerDefinition.<Id, Document, Id, Document>builder(Id.class, Document.class, Id.class, Document.class)
                                                                                                  .applicationId(APP_ID)
                                                                                                  .keySerializer(Serializer.AVRO)
                                                                                                  .valueSerializer(Serializer.AVRO)
                                                                                                  .keyDeserializer(Deserializer.AVRO)
                                                                                                  .valueDeserializer(Deserializer.AVRO)
                                                                                                  .inputTopic("input")
                                                                                                  .outputTopic("output")
                                                                                                  .bootstrapServers(kafka.getBootstrapServers())
                                                                                                  .build());
                  TestConsumer<byte[], byte[]> consumer = start("output", ByteArrayDeserializer.class, ByteArrayDeserializer.class)) {
                
                //Serde<KeyPojo> keySerde = new OfflineAvroSerde<>(KeyPojo.class);
                //Serde<ValuePojo> valueSerde = new OfflineAvroSerde<>(ValuePojo.class);
                Id id = new Id(568L);
                Document document = new Document(568L, "file.txt", 123);
                streamer.start();

                byte[] idBytes = assertDoesNotThrow(() -> {
                    ByteBuffer idBuffer = id.toByteBuffer();
                    byte[] bArray = new byte[idBuffer.remaining()];
                    idBuffer.get(bArray);
                    return bArray;
                });

                byte[] docBytes = assertDoesNotThrow(()-> {
                    ByteBuffer docBuffer = document.toByteBuffer();
                    byte[] bArray = new byte[docBuffer.remaining()];
                    docBuffer.get(bArray);
                    return bArray;
                });
                
                sendMessage("input", idBytes, ByteArraySerializer.class , docBytes, ByteArraySerializer.class);
                consumer.next((key, value) -> {
                    try {
                        assertEquals(new Id(568L), Id.fromByteBuffer(ByteBuffer.wrap(key)));
                        assertEquals(new Document(568L, "file.txt", 123), Document.fromByteBuffer(ByteBuffer.wrap(value)));
                    } catch (Exception ex) {
                        fail(ex);
                    }
                }, Duration.ofSeconds(160));
            }
        }

        @Test
        @DisplayName("Protobuf")
        void protobufTest() {
            createTopics("input", "output");
            try (Streamer<Key, Person, Key, Person> streamer = new Streamer<>(StreamerDefinition.<Key, Person, Key, Person>builder(Key.class, Person.class, Key.class, Person.class)
                                                                                                .applicationId(APP_ID)
                                                                                                .keySerializer(Serializer.PROTOBUF)
                                                                                                .valueSerializer(Serializer.PROTOBUF)
                                                                                                .keyDeserializer(Deserializer.PROTOBUF)
                                                                                                .valueDeserializer(Deserializer.PROTOBUF)
                                                                                                .inputTopic("input")
                                                                                                .outputTopic("output")
                                                                                                .bootstrapServers(kafka.getBootstrapServers())
                                                                                                .build());
                  TestConsumer<byte[], byte[]> consumer = start("output", ByteArrayDeserializer.class, ByteArrayDeserializer.class)) {
                
                streamer.start();
                Key protoKey = Key.newBuilder()
                             .setKey("000001")
                             .build();
                Person person = Person.newBuilder()
                                      .setId(1)
                                      .setName("user")
                                      .setEmail("user@user.com")
                                      .build();
                sendMessage("input", protoKey.toByteArray(), ByteArraySerializer.class , person.toByteArray(), ByteArraySerializer.class);
                consumer.next((key, value) -> {
                    try {
                        assertEquals(protoKey, Key.parseFrom(key));
                        assertEquals(person, Person.parseFrom(value));
                    } catch (Exception ex) {
                        fail(ex);
                    }
                }, Duration.ofSeconds(160));
            }
        }
    }
}