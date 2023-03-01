package io.vepo.datamotion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class StreamerConfigurationTest {

    @Test
    void getMainClassFullQualifiedNameTest() {
        StreamerDefinition definition = new StreamerDefinition();
        definition.setPackageName("io.vepo.streamer");
        definition.setId("hello-world");
        StreamerConfiguration config = new StreamerConfiguration(definition);

        assertEquals("io.vepo.streamer.HelloWorldStreamer", config.getMainClassFullQualifiedName());
    }
}