package cn.syntomic.qflink.common.codec.ser;

import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonStringSerializationSchema<T> implements SerializationSchema<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(T element) {
        try {
            return MAPPER.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Serialize " + element.toString() + " to json string failed", e);
        }
    }
}
