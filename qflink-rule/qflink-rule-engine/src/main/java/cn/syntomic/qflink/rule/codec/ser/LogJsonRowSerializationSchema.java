package cn.syntomic.qflink.rule.codec.ser;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/** Serialization row according to TypeInformation */
public class LogJsonRowSerializationSchema implements SerializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final SerializationSchema<Row> serializationSchema;

    public LogJsonRowSerializationSchema(TypeInformation<Row> typeInfo) {

        if (typeInfo instanceof RowTypeInfo) {
            serializationSchema =
                    JsonRowSerializationSchema.builder().withTypeInfo(typeInfo).build();
        } else {
            serializationSchema = new DynamicJsonRowSerializationSchema();
        }
    }

    @Override
    public byte[] serialize(Row element) {
        return serializationSchema.serialize(element);
    }
}
