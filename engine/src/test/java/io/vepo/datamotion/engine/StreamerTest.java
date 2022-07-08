package io.vepo.datamotion.engine;

import io.vepo.datamotion.configuration.StreamerDefinition;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
class StreamerTest {

    private static final Logger logger = LoggerFactory.getLogger(StreamerTest.class);
    @Container
    public KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.0"));
    private static final String APP_ID = "STREAMER_TEST_APP";

    @BeforeEach
    void cleanup() throws IOException {
        Path tempFolder = Paths.get(System.getProperty("java.io.tmpdir"), "kafka-streams", APP_ID);
        if (tempFolder.toFile().exists()) {
            Files.walk(tempFolder)
                 .sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .forEach(File::delete);
        }
    }

    @Test
    void passthruTest() {
        createTopics("input", "output");
        try (Streamer streamer = new Streamer(StreamerDefinition.builder()
                                                                .applicationId(APP_ID)
                                                                .inputTopic("input")
                                                                .outputTopic("output")
                                                                .bootstrapServers(kafka.getBootstrapServers())
                                                                .build());
             TestConsumer<String, Long> consumer = start("output", StringDeserializer.class, LongDeserializer.class)) {
            streamer.start();
            sendMessage("input", "Hello World!");
            consumer.next((key, value) -> System.out.println("Key=" + key + " value=" + value), Duration.ofSeconds(160));
            logger.info("Test finished!");
        }
    }

    private void createTopics(String... topics) {
        logger.info("Creating topics! topics={}", topics);
        List<NewTopic> newTopics =
                Arrays.stream(topics)
                      .map(topic -> new NewTopic(topic, 6, (short) 1))
                      .collect(Collectors.toList());
        try (AdminClient admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            CreateTopicsResult results = admin.createTopics(newTopics);
            logger.info("Create topic results={}", results.values());
        }
    }

    private <K, V> TestConsumer<K, V> start(String topic, Class<? extends Deserializer<K>> keyDeserializerClass,
                                            Class<? extends Deserializer<V>> valueDeserialzierClass) {
        logger.info("Starting consumer...");
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
            Executors.newSingleThreadExecutor()
                     .submit(() -> {
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

        public void next(BiConsumer<K, V> consumer, Duration timeout) {
            long start = System.nanoTime();
            while (messages.isEmpty()) {
                if (timeout.toNanos() < System.nanoTime() - start) {
                    fail("No message received! timeout=" + timeout);
                }
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
        logger.info("Sending message! topic={} message={}", topic, message);
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (Producer<String, String> producer = new KafkaProducer<>(configProperties)) {
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic, message);
            Future<RecordMetadata> results = producer.send(rec);
            RecordMetadata metadata = results.get();
            logger.info("Message sent! metadata={}", metadata);
        } catch (InterruptedException e) {
            fail("Error sending message!", e);
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
        }
    }

}