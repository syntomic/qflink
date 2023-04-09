package cn.syntomic.qflink.rule.codec.deser;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.types.Row;

import org.slf4j.Logger;

import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.LogFormat;

/** Deserialization log according format and avro schema */
public class LogRowDeserializationSchema implements DeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            org.slf4j.LoggerFactory.getLogger(LogRowDeserializationSchema.class);

    private final DeserializationSchema<Row> deserializationSchema;

    public LogRowDeserializationSchema(LogFormat format, String schema) {

        switch (format) {
            case RAW:
                deserializationSchema = new SimpleRowDeserializationSchema();
                break;
            case AVRO:
                deserializationSchema = new AvroRowDeserializationSchema(schema);
                break;
            case JSON:
            default:
                deserializationSchema =
                        new JsonRowDeserializationSchema.Builder(
                                        AvroSchemaConverter.<Row>convertToTypeInfo(schema))
                                .build();
                break;
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            return deserializationSchema.deserialize(message);
        } catch (Exception e) {
            LOG.error("Rule Engine: log deser error ", e);
            // ! Watch out NullPointer
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }
}
