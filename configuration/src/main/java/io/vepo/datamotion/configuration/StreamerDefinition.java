package io.vepo.datamotion.configuration;

import java.util.Objects;

public class StreamerDefinition<KI, VI, KO, VO> {

    public static class StreamerDefinitionBuilder<KI, VI, KO, VO> {
        private final Class<KI> inputKeyClass;
        private final Class<VI> inputValueClass;
        private final Class<KO> outputKeyClass;
        private final Class<VO> outputValueClass;
        private Serializer keySerializer;
        private Serializer valueSerializer;
        private Deserializer keyDeserializer;
        private Deserializer valueDeserializer;
        private String bootstrapServers;
        private String applicationId;
        private String inputTopic;
        private String outputTopic;

        private StreamerDefinitionBuilder(Class<KI> inputKeyClass, Class<VI> inputValueClass, Class<KO> outputKeyClass, Class<VO> outputValueClass) {
            this.inputKeyClass = inputKeyClass;
            this.inputValueClass = inputValueClass;
            this.outputKeyClass = outputKeyClass;
            this.outputValueClass = outputValueClass;
        }

        public StreamerDefinitionBuilder<KI, VI, KO, VO> keySerializer(Serializer keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public StreamerDefinitionBuilder<KI, VI, KO, VO> valueSerializer(Serializer valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public StreamerDefinitionBuilder<KI, VI, KO, VO> keyDeserializer(Deserializer keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        public StreamerDefinitionBuilder<KI, VI, KO, VO> valueDeserializer(Deserializer valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        public StreamerDefinitionBuilder<KI, VI, KO, VO> bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public StreamerDefinitionBuilder<KI, VI, KO, VO> applicationId(String applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        public StreamerDefinitionBuilder<KI, VI, KO, VO> inputTopic(String inputTopic) {
            this.inputTopic = inputTopic;
            return this;
        }

        public StreamerDefinitionBuilder<KI, VI, KO, VO> outputTopic(String outputTopic) {
            this.outputTopic = outputTopic;
            return this;
        }

        public StreamerDefinition<KI, VI, KO, VO> build() {
            Objects.requireNonNull(this.keySerializer, "keySerializer is a required value!");
            Objects.requireNonNull(this.valueSerializer, "valueSerializer is a required value!");
            Objects.requireNonNull(this.keyDeserializer, "keyDeserializer is a required value!");
            Objects.requireNonNull(this.valueDeserializer, "valueDeserializer is a required value!");
            Objects.requireNonNull(this.bootstrapServers, "bootstrapServers is a required value!");
            Objects.requireNonNull(this.inputTopic, "inputTopic is a required value!");
            Objects.requireNonNull(this.outputTopic, "outputTopic is a required value!");
            return new StreamerDefinition<>(this);
        }
    }

    public static <KI, VI, KO, VO> StreamerDefinitionBuilder<KI, VI, KO, VO> builder(Class<KI> inputKeyClass, 
                                                                                     Class<VI> inputValueClass, 
                                                                                     Class<KO> outputKeyClass, 
                                                                                     Class<VO> outputValueClass) {
        return new StreamerDefinitionBuilder<>(inputKeyClass, inputValueClass, outputKeyClass, outputValueClass);
    }

    private final Class<KI> inputKeyClass;
    private final Class<VI> inputValueClass;
    private final Class<KO> outputKeyClass;
    private final Class<VO> outputValueClass;
    private final Serializer keySerializer;
    private final Serializer valueSerializer;
    private final Deserializer keyDeserializer;
    private final Deserializer valueDeserializer;
    private final String bootstrapServers;
    private final String applicationId;
    private final String inputTopic;
    private final String outputTopic;

    private StreamerDefinition(StreamerDefinitionBuilder<KI, VI, KO, VO> builder) {
        this.inputKeyClass = builder.inputKeyClass;
        this.inputValueClass = builder.inputValueClass;
        this.outputKeyClass = builder.outputKeyClass;
        this.outputValueClass = builder.outputValueClass;

        this.bootstrapServers = builder.bootstrapServers;
        this.applicationId = builder.applicationId;
        this.inputTopic = builder.inputTopic;
        this.outputTopic = builder.outputTopic;
        this.keySerializer = builder.keySerializer;
        this.valueSerializer = builder.valueSerializer;
        this.keyDeserializer = builder.keyDeserializer;
        this.valueDeserializer = builder.valueDeserializer;
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

    public Serializer getKeySerializer() {
        return keySerializer;
    }

    public Class<KI> getInputKeyClass() {
        return inputKeyClass;
    }

    public Class<VI> getInputValueClass() {
        return inputValueClass;
    }

    public Class<KO> getOutputKeyClass() {
        return outputKeyClass;
    }

    public Class<VO> getOutputValueClass() {
        return outputValueClass;
    }

    public Deserializer getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer getValueDeserializer() {
        return valueDeserializer;
    }

    public Serializer getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        } else {
            StreamerDefinition<?, ?, ?, ?> that = (StreamerDefinition<?, ?, ?, ?>) o;
            return Objects.equals(keySerializer, that.keySerializer) &&
                    Objects.equals(valueSerializer, that.valueSerializer) &&
                    Objects.equals(keyDeserializer, that.keyDeserializer) &&
                    Objects.equals(valueDeserializer, that.valueDeserializer) &&
                    Objects.equals(bootstrapServers, that.bootstrapServers) &&
                    Objects.equals(applicationId, that.applicationId) &&
                    Objects.equals(inputTopic, that.inputTopic) &&
                    Objects.equals(outputTopic, that.outputTopic);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(keySerializer, valueSerializer, keyDeserializer, valueDeserializer, bootstrapServers,
                applicationId, inputTopic, outputTopic);
    }

    @Override
    public String toString() {
        return String.format(
                "StreamerDefinition[keySerializer=%s, valueSerializer=%s, keyDeserializer=%s, valueDeserializer=%s, bootstrapServers='%s', applicationId='%s', inputTopic='%s', outputTopic='%s']",
                keySerializer, valueSerializer, keyDeserializer, valueDeserializer, bootstrapServers, applicationId, inputTopic, outputTopic);
    }
}
