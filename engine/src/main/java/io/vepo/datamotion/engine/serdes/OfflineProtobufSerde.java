package io.vepo.datamotion.engine.serdes;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.protobuf.GeneratedMessageV3;

public class OfflineProtobufSerde<T extends GeneratedMessageV3> implements Serde<T> {

    private class OfflineProtobufSerializer implements Serializer<T> {

        @Override
        public byte[] serialize(String topic, T data) {
            return data.toByteArray();
        }
    }

    private class OfflineProtobufDeserializer implements Deserializer<T> {

        @Override
        public T deserialize(String topic, byte[] data) {
            try {
                return (T) parseFromMethod.invoke(data);
            } catch (Throwable e) {
                throw new KafkaException("Could not parse protobuf", e);
            }
        }
    }

    private MethodHandle parseFromMethod;

    public OfflineProtobufSerde(Class<T> valueClass) {
        MethodHandles.Lookup lookup = MethodHandles.publicLookup();
        try {
            parseFromMethod = lookup.findStatic(valueClass,
                                                "parseFrom",
                                                MethodType.methodType(valueClass, byte[].class));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not find parseFrom method!", e);
        }
    }

    @Override
    public Serializer<T> serializer() {
        return new OfflineProtobufSerializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new OfflineProtobufDeserializer();
    }

}
