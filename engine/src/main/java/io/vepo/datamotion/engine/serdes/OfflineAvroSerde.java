package io.vepo.datamotion.engine.serdes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class OfflineAvroSerde<T> implements Serde<T> {

    private final Class<T> valueClass;

    public OfflineAvroSerde(Class<T> valueClass) {
        this.valueClass = valueClass;
    }

    public class OfflineAvroSerializer<C> implements Serializer<C> {

        @Override
        public byte[] serialize(String topic, C data) {
            if (SpecificRecordBase.class.isAssignableFrom(valueClass)) {
                MethodHandles.Lookup publicLookup = MethodHandles.publicLookup();
                try {
                    MethodHandle toByteBuffer = publicLookup.findVirtual(valueClass,
                                                                         "toByteBuffer",
                                                                         MethodType.methodType(ByteBuffer.class));
                    return ((ByteBuffer) toByteBuffer.invoke(data)).array();
                } catch (Throwable e) {
                    throw new KafkaException("Could not serialize AVRO!", e);
                }
            } else {
                final Schema schema = ReflectData.AllowNull.get().getSchema(valueClass);
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    ReflectDatumWriter<C> datumWriter = new ReflectDatumWriter<>(schema);
                    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                    datumWriter.write(data, encoder);
                    encoder.flush();

                    return outputStream.toByteArray();
                } catch (IOException ioe) {
                    throw new KafkaException("Could not serialize AVRO!", ioe);
                }
            }
        }
    }

    public class OfflineAvroDeserializer<C> implements Deserializer<C> {

        @Override
        public C deserialize(String topic, byte[] data) {
            if (SpecificRecordBase.class.isAssignableFrom(valueClass)) {
                try {
                    MethodHandles.Lookup publicLookup = MethodHandles.publicLookup();
                    MethodHandle fromByteBuffer = publicLookup.findStatic(valueClass, "fromByteBuffer",
                                                                          MethodType.methodType(valueClass,
                                                                                                ByteBuffer.class));

                    return (C) fromByteBuffer.invoke(ByteBuffer.wrap(data));
                } catch (Throwable ex) {
                    throw new KafkaException("Could not deserialize AVRO!", ex);
                }
            } else {
                final Schema schema = ReflectData.AllowNull.get()
                                                           .getSchema(valueClass);
                ReflectDatumReader<C> datumReader = new ReflectDatumReader<>(schema);
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
                    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
                    return datumReader.read(null, decoder);
                } catch (IOException | AvroRuntimeException ex) {
                    throw new KafkaException("Could not deserialize AVRO!", ex);
                }
            }
        }
    }

    @Override
    public Serializer<T> serializer() {
        return new OfflineAvroSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new OfflineAvroDeserializer<>();
    }

}
