package cn.syntomic.qflink.rule.codec.ser;

import java.util.stream.Collectors;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/** Convert Row to Map<String, Object>, then serialize */
public class DynamicJsonRowSerializationSchema implements SerializationSchema<Row> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    @SuppressWarnings("null")
    public byte[] serialize(Row row) {
        try {
            return mapper.writeValueAsBytes(
                    row.getFieldNames(true).stream()
                            .collect(Collectors.toMap(name -> name, row::getField)));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not serialize row '"
                            + row
                            + "'. "
                            + "Make sure that the schema matches the input.",
                    e);
        }
    }
}
