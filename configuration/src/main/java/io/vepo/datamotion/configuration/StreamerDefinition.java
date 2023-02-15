package io.vepo.datamotion.configuration;

import java.util.Objects;

public class StreamerDefinition<I, O> {

    public static class StreamerDefinitionBuilder<I, O> {
        private final Serializer serializer;
        private final Deserializer deserializer;
        private String bootstrapServers;
        private String applicationId;
        private String inputTopic;
        private String outputTopic;

        private StreamerDefinitionBuilder(Serializer serializer, Deserializer deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        public StreamerDefinitionBuilder<I, O> bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public StreamerDefinitionBuilder<I, O> applicationId(String applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        public StreamerDefinitionBuilder<I, O> inputTopic(String inputTopic) {
            this.inputTopic = inputTopic;
            return this;
        }

        public StreamerDefinitionBuilder<I, O> outputTopic(String outputTopic) {
            this.outputTopic = outputTopic;
            return this;
        }

        public StreamerDefinition<I, O> build() {
            return new StreamerDefinition<>(this);
        }
    }

    public static <I, O> StreamerDefinitionBuilder<I, O> builder(Serializer serializer, Deserializer deserializer) {
        return new StreamerDefinitionBuilder<>(serializer, deserializer);
    }

    private final Serializer serializer;
    private final Deserializer deserializer;
    private final String bootstrapServers;
    private final String applicationId;
    private final String inputTopic;
    private final String outputTopic;

    private StreamerDefinition(StreamerDefinitionBuilder<I, O> builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.applicationId = builder.applicationId;
        this.inputTopic = builder.inputTopic;
        this.outputTopic = builder.outputTopic;
        this.serializer = builder.serializer;
        this.deserializer = builder.deserializer;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public Deserializer getDeserializer() {
        return deserializer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamerDefinition<?, ?> that = (StreamerDefinition<?, ?>) o;
        return Objects.equals(serializer, that.serializer) && 
                Objects.equals(deserializer, that.deserializer) &&        
                Objects.equals(bootstrapServers, that.bootstrapServers) &&
                Objects.equals(applicationId, that.applicationId) &&
                Objects.equals(inputTopic, that.inputTopic) &&
                Objects.equals(outputTopic, that.outputTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bootstrapServers, applicationId, inputTopic, outputTopic);
    }

    @Override
    public String toString() {
        return String.format("StreamerDefinition[serializer=%s, deserializer=%s, bootstrapServers='%s', applicationId='%s', inputTopic='%s', outputTopic='%s']",
                             serializer, deserializer, bootstrapServers, applicationId, inputTopic, outputTopic);
    }
}
