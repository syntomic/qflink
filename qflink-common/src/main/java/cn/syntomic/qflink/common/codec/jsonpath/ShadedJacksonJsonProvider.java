package cn.syntomic.qflink.common.codec.jsonpath;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;

import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.spi.json.AbstractJsonProvider;

/**
 * Use flink shaded jackson. Flink shaded jsonpath is a optional dependency of flink runtime, we
 * cannot use it directly.
 */
public class ShadedJacksonJsonProvider extends AbstractJsonProvider {

    private static final ObjectMapper defaultObjectMapper = new ObjectMapper();
    private static final ObjectReader defaultObjectReader =
            defaultObjectMapper.reader().forType(Object.class);

    protected ObjectMapper objectMapper;
    protected ObjectReader objectReader;

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    /** Initialize the JacksonProvider with the default ObjectMapper and ObjectReader */
    public ShadedJacksonJsonProvider() {
        this(defaultObjectMapper, defaultObjectReader);
    }

    /**
     * Initialize the JacksonProvider with a custom ObjectMapper.
     *
     * @param objectMapper the ObjectMapper to use
     */
    public ShadedJacksonJsonProvider(ObjectMapper objectMapper) {
        this(objectMapper, objectMapper.reader().forType(Object.class));
    }

    /**
     * Initialize the JacksonProvider with a custom ObjectMapper and ObjectReader.
     *
     * @param objectMapper the ObjectMapper to use
     * @param objectReader the ObjectReader to use
     */
    public ShadedJacksonJsonProvider(ObjectMapper objectMapper, ObjectReader objectReader) {
        this.objectMapper = objectMapper;
        this.objectReader = objectReader;
    }

    @Override
    public Object parse(String json) throws InvalidJsonException {
        try {
            // ? readValue vs readTree
            return objectReader.readValue(json);
        } catch (IOException e) {
            throw new InvalidJsonException(e, json);
        }
    }

    @Override
    public Object parse(byte[] json) throws InvalidJsonException {
        try {
            return objectReader.readValue(json);
        } catch (IOException e) {
            throw new InvalidJsonException(e, new String(json, StandardCharsets.UTF_8));
        }
    }

    @Override
    public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException {
        try {
            return objectReader.readValue(new InputStreamReader(jsonStream, charset));
        } catch (IOException e) {
            throw new InvalidJsonException(e);
        }
    }

    @Override
    public String toJson(Object obj) {
        StringWriter writer = new StringWriter();
        try {
            JsonGenerator generator = objectMapper.getFactory().createGenerator(writer);
            objectMapper.writeValue(generator, obj);
            writer.flush();
            writer.close();
            generator.close();
            return writer.getBuffer().toString();
        } catch (IOException e) {
            throw new InvalidJsonException(e);
        }
    }

    @Override
    public List<Object> createArray() {
        return new LinkedList<>();
    }

    @Override
    public Object createMap() {
        return new LinkedHashMap<String, Object>();
    }
}
