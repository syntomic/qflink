package cn.syntomic.qflink.common.codec.deser;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonPojoDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonPojoDeserializationSchema.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Class<T> clazz;
    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    public JsonPojoDeserializationSchema(Class<T> clazz, boolean ignoreParseErrors) {
        this.clazz = clazz;
        this.ignoreParseErrors = ignoreParseErrors;
    }

    public JsonPojoDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
        this.ignoreParseErrors = false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return Types.POJO(clazz);
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        try {
            return MAPPER.readValue(message, clazz);
        } catch (Exception e) {
            String errorMsg = String.format("Deserialize Json to Pojo %s failed", clazz.getName());
            if (ignoreParseErrors) {
                LOG.error(errorMsg, e);
                return null;
            }

            throw new IOException(errorMsg, e);
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }
}
