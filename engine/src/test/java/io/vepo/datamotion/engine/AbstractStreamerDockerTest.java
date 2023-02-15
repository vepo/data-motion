package io.vepo.datamotion.engine;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("DOCKER")
@Testcontainers
abstract class AbstractStreamerDockerTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractStreamerDockerTest.class);
    protected static final String APP_ID = "STREAMER_TEST_APP";

    public KafkaContainer kafka;

    @BeforeEach
    void cleanup() throws IOException {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.0"))
                                                  .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                                                  .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        kafka.start();
        Path tempFolder = Paths.get(System.getProperty("java.io.tmpdir"), "kafka-streams", APP_ID);
        if (tempFolder.toFile().exists()) {
            Files.walk(tempFolder)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @AfterEach
    void shutdown() {
        kafka.stop();
        kafka.close();
    }

    protected void createTopics(String... topics) {
        logger.info("Creating topics! topics={}", Arrays.toString(topics));
        try (AdminClient admin = AdminClient.create(
                Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            Arrays.stream(topics)
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .forEach(command -> {
                        CreateTopicsResult results = admin.createTopics(Arrays.asList(command));
                        logger.info("Create topic results={}", results.values());
                        try {
                            results.all().get();
                            admin.describeTopics(Arrays.asList(command.name()))
                                    .allTopicNames()
                                    .get()
                                    .forEach((key, value) -> {
                                        logger.info("Topic information! id={} value={}", key, value);
                                    });
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (ExecutionException e) {
                            logger.error("Error creating topic!", e);
                        }
                    });
        }
    }

    

    protected TestConsumer<Object, Object> startPojo(String topic, Class keyDeserializerClass, Class valueDeserialzierClass) {
        return start(topic, keyDeserializerClass, valueDeserialzierClass);
    }

    protected <K, V> TestConsumer<K, V> start(String topic, Class<? extends Deserializer<K>> keyDeserializerClass, Class<? extends Deserializer<V>> valueDeserialzierClass) {
        logger.info("Starting consumer...");
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "Test-Consumer-" + UUID.randomUUID().toString());
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserialzierClass);

        return new TestConsumer<K, V>(new KafkaConsumer<>(configProperties), topic);
    }

    protected class TestConsumer<K, V> implements Closeable {

        private KafkaConsumer<K, V> consumer;
        private List<ConsumerRecord<K, V>> messages = Collections.synchronizedList(new LinkedList<>());
        private AtomicBoolean running = new AtomicBoolean(true);
        private CountDownLatch latch = new CountDownLatch(1);

        public TestConsumer(KafkaConsumer<K, V> consumer, String topic) {
            this.consumer = consumer;
            consumer.subscribe(Arrays.asList(topic));
            Executors.newSingleThreadExecutor()
                    .submit(() -> {
                        while (running.get()) {
                            consumer.poll(Duration.ofMillis(500))
                                    .forEach(record -> {
                                        logger.info("Message received! record={}", record);
                                        messages.add(record);
                                    });
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        latch.countDown();
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
            running.set(false);
            try {
                latch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            consumer.close();
        }

    }

    protected <V> void sendMessage(String topic, V message) {
        sendMessage(topic, null, message);
    }

    
    protected <K, V> void sendMessage(String topic, K key, V message) {
        sendMessage(topic, key, serializerFor(key), message, serializerFor(message));
    }

    protected <K, V> void sendMessage(String topic, K key, Class<?> keySerializer, V message, Class<?> valueSerializer) {
        logger.info("Sending message! topic={} message={}", topic, message);
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        try (Producer<K, V> producer = new KafkaProducer<>(configProperties)) {
            ProducerRecord<K, V> rec = new ProducerRecord<>(topic, key, message);
            Future<RecordMetadata> results = producer.send(rec);
            RecordMetadata metadata = results.get();
            logger.info("Message sent! metadata={}", metadata);
        } catch (InterruptedException e) {
            fail("Error sending message!", e);
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Class<?> serializerFor(Object value) {
        if (value instanceof String) {
            return StringSerializer.class;
        } else if (value instanceof Long) {
            return LongSerializer.class;
        } else if (value instanceof Integer) {
            return IntegerSerializer.class;
        } else {
            throw new IllegalStateException("Serializer not implemented! class=" + value.getClass());
        }
    }
}
