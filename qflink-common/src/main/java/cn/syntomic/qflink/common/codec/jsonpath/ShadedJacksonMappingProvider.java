package cn.syntomic.qflink.common.codec.jsonpath;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.mapper.MappingException;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

/**
 * Use flink shaded jackson. Flink shaded jsonpath is a optional dependency of flink runtime, we
 * cannot use it directly.
 */
public class ShadedJacksonMappingProvider implements MappingProvider {

    private final ObjectMapper objectMapper;

    public ShadedJacksonMappingProvider() {
        this(new ObjectMapper());
    }

    public ShadedJacksonMappingProvider(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public <T> T map(Object source, Class<T> targetType, Configuration configuration) {
        if (source == null) {
            return null;
        }
        try {
            return objectMapper.convertValue(source, targetType);
        } catch (Exception e) {
            throw new MappingException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T map(Object source, final TypeRef<T> targetType, Configuration configuration) {
        if (source == null) {
            return null;
        }
        JavaType type = objectMapper.getTypeFactory().constructType(targetType.getType());

        try {
            return (T) objectMapper.convertValue(source, type);
        } catch (Exception e) {
            throw new MappingException(e);
        }
    }
}
