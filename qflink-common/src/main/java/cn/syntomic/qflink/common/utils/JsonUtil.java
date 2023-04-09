package cn.syntomic.qflink.common.utils;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.cache.LRUCache;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import cn.syntomic.qflink.common.codec.deser.NestedListDeserializer;
import cn.syntomic.qflink.common.codec.deser.NestedMapDeserializer;
import cn.syntomic.qflink.common.codec.deser.QArrayList;
import cn.syntomic.qflink.common.codec.deser.QLinkedHashMap;
import cn.syntomic.qflink.common.codec.deser.StringCleanDeserializer;
import cn.syntomic.qflink.common.codec.jsonpath.ShadedJacksonJsonProvider;
import cn.syntomic.qflink.common.codec.jsonpath.ShadedJacksonMappingProvider;
import cn.syntomic.qflink.common.entity.Field;
import cn.syntomic.qflink.common.entity.Field.FieldType;

public class JsonUtil {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Common mapper for mapper is thread safe
     *
     * @return
     */
    public static ObjectMapper getCommonMapper() {
        return MAPPER;
    }

    /**
     * Deserialize nest json to string map
     *
     * @param cleanStrings dirty strings cleaned to null
     * @return
     */
    public static ObjectMapper createNestedMapper(@Nullable String[] cleanStrings) {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();

        module.addDeserializer(QLinkedHashMap.class, new NestedMapDeserializer());
        module.addDeserializer(QArrayList.class, new NestedListDeserializer());
        if (cleanStrings != null && cleanStrings.length != 0) {
            module.addDeserializer(String.class, new StringCleanDeserializer(cleanStrings));
        }

        mapper.registerModule(module);

        return mapper;
    }

    /**
     * Init Json path configuration
     *
     * @param cleanStrings
     * @param limit
     */
    public static void openJsonPath(@Nullable String[] cleanStrings, int limit) {
        Configuration.setDefaults(
                new Configuration.Defaults() {

                    private final JsonProvider jsonProvider =
                            new ShadedJacksonJsonProvider(
                                    JsonUtil.createNestedMapper(cleanStrings));
                    private final MappingProvider mappingProvider =
                            new ShadedJacksonMappingProvider(JsonUtil.createNestedMapper(null));

                    @Override
                    public JsonProvider jsonProvider() {
                        return jsonProvider;
                    }

                    @Override
                    public MappingProvider mappingProvider() {
                        return mappingProvider;
                    }

                    @Override
                    public Set<Option> options() {
                        return EnumSet.of(Option.DEFAULT_PATH_LEAF_TO_NULL);
                    }
                });

        if (limit > 0) {
            CacheProvider.setCache(new LRUCache(limit));
        }
    }

    /**
     * Eval multi json paths to consumer once
     *
     * @param json
     * @param fields
     * @param consumer
     */
    public static void evalJsonPaths(
            String json, Field[] fields, BiConsumer<String, Object> consumer) {

        DocumentContext ctx = JsonPath.parse(json);

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];

            Object result;
            if (field.getType() == FieldType.STRING) {
                result = ctx.read(field.getExpr());
                // ! maybe nested object
                if (result != null && !(result instanceof String)) {
                    result = ctx.configuration().jsonProvider().toJson(result);
                }
            } else {
                result = ctx.read(field.getExpr(), field.getType().getClazz());
            }

            if (result == null) {
                consumer.accept(field.getName(), field.getDefaulz());
            } else {
                consumer.accept(field.getName(), result);
            }
        }
    }

    private JsonUtil() {}
}
