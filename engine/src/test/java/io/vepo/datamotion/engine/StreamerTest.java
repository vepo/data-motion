package io.vepo.datamotion.engine;

import io.vepo.datamotion.configuration.StreamerDefinition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

@Testcontainers
class StreamerTest {
    @Container
    public KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7"));
    private static final String APP_ID = "STREAMER_TEST_APP";

    @BeforeEach
    void cleanup() throws IOException {
        Files.walk(Paths.get(System.getProperty("java.io.tmpdir"), "kafka-streams", APP_ID))
             .sorted(Comparator.reverseOrder())
             .map(Path::toFile)
             .forEach(File::delete);

    }

    @Test
    void passthruTest() {
        try (Streamer streamer = new Streamer(StreamerDefinition.builder()
                                                                .applicationId(APP_ID)
                                                                .inputTopic("input")
                                                                .outputTopic("output")
                                                                .bootstrapServers(kafka.getBootstrapServers())
                                                                .build());
             TestConsumer<String, Long> consumer = start("output", StringDeserializer.class, LongDeserializer.class)) {
            Executors.newSingleThreadExecutor().submit(streamer::start);
            sendMessage("input", "Hello World!");
            consumer.next((key, value) -> System.out.println("Key=" + key + " value=" + value));
        }
    }

    private <K, V> TestConsumer<K, V> start(String topic, Class<? extends Deserializer<K>> keyDeserializerClass,
                                            Class<? extends Deserializer<V>> valueDeserialzierClass) {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "Test-Consumer-" + UUID.randomUUID().toString());
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserialzierClass);

        return new TestConsumer<K, V>(new KafkaConsumer<>(configProperties), topic);
    }

    private class TestConsumer<K, V> implements Closeable {

        private KafkaConsumer<K, V> consumer;
        private List<ConsumerRecord<K, V>> messages = Collections.synchronizedList(new LinkedList<>());
        private AtomicBoolean running = new AtomicBoolean(true);

        public TestConsumer(KafkaConsumer<K, V> consumer, String topic) {
            this.consumer = consumer;
            consumer.subscribe(Arrays.asList(topic));
            Executors.newSingleThreadExecutor().submit(() -> {
                while (running.get()) {
                    consumer.poll(Duration.ofMillis(500)).forEach(record -> messages.add(record));
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        public void next(BiConsumer<K, V> consumer) {
            while (messages.isEmpty()) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            ConsumerRecord<K, V> message = messages.remove(0);
            consumer.accept(message.key(), message.value());
        }

        @Override
        public void close() {
            consumer.close();
        }

    }

    private void sendMessage(String topic, String message) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (Producer<String, String> producer = new KafkaProducer<>(configProperties)) {
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic, message);
            Future<RecordMetadata> results = producer.send(rec);
            RecordMetadata metadata = results.get();
            System.out.println(metadata);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
        }
    }

}