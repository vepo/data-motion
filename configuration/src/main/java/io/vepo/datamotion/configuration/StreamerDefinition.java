package io.vepo.datamotion.configuration;

import java.util.Objects;

public class StreamerDefinition {

    public static class StreamerDefinitionBuilder {
        private String bootstrapServers;
        private String applicationId;
        private String inputTopic;
        private String outputTopic;

        private StreamerDefinitionBuilder() {
        }

        public StreamerDefinitionBuilder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public StreamerDefinitionBuilder applicationId(String applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        public StreamerDefinitionBuilder inputTopic(String inputTopic) {
            this.inputTopic = inputTopic;
            return this;
        }

        public StreamerDefinitionBuilder outputTopic(String outputTopic) {
            this.outputTopic = outputTopic;
            return this;
        }

        public StreamerDefinition build() {
            return new StreamerDefinition(this);
        }
    }

    public static StreamerDefinitionBuilder builder() {
        return new StreamerDefinitionBuilder();
    }

    private final String bootstrapServers;
    private final String applicationId;
    private final String inputTopic;
    private final String outputTopic;

    private StreamerDefinition(StreamerDefinitionBuilder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.applicationId = builder.applicationId;
        this.inputTopic = builder.inputTopic;
        this.outputTopic = builder.outputTopic;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamerDefinition that = (StreamerDefinition) o;
        return Objects.equals(bootstrapServers, that.bootstrapServers) &&
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
        return String.format("StreamerDefinition[bootstrapServers='%s', applicationId='%s', inputTopic='%s', outputTopic='%s']",
                             bootstrapServers, applicationId, inputTopic, outputTopic);
    }
}
