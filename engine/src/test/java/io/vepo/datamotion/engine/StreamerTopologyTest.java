package io.vepo.datamotion.engine;

import io.vepo.datamotion.configuration.StreamerDefinition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.vepo.datamotion.engine.Streamer.buildKafkaTopology;
import static io.vepo.datamotion.engine.Streamer.buildStreamProperties;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Streamer Topology")
public class StreamerTopologyTest {
    private static final String APP_ID = "STREAMER_TEST_APP";

    @Test
    @DisplayName("Passthru")
    void passthruTest() {

        StreamerDefinition definition = StreamerDefinition.builder()
                                                          .applicationId(APP_ID)
                                                          .inputTopic("input")
                                                          .outputTopic("output")
                                                          .bootstrapServers("localhost:90921")
                                                          .build();
        TopologyTestDriver testDriver = new TopologyTestDriver(buildKafkaTopology(definition),
                                                               buildStreamProperties(definition));
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output", Serdes.String().deserializer(), Serdes.String().deserializer());
        inputTopic.pipeInput("key", "Hello World!");
        assertThat(outputTopic.readKeyValue()).isEqualTo(KeyValue.pair("key", "Hello World!"));

    }
}
