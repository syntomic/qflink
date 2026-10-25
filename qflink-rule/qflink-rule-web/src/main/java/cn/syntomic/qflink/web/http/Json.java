package cn.syntomic.qflink.web.http;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** The one object mapper of the console, the rules are kept as trees and never lose a field. */
public class Json {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static ObjectNode object() {
        return MAPPER.createObjectNode();
    }

    public static ArrayNode array() {
        return MAPPER.createArrayNode();
    }

    public static JsonNode read(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Not a json document: " + e.getOriginalMessage(), e);
        }
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot write " + value, e);
        }
    }

    public static String writePretty(Object value) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot write " + value, e);
        }
    }

    /** A json object out of alternating keys and values, the values are written as they are. */
    public static ObjectNode of(Object... keyValues) {
        ObjectNode node = object();
        for (int i = 0; i < keyValues.length; i += 2) {
            node.set(String.valueOf(keyValues[i]), MAPPER.valueToTree(keyValues[i + 1]));
        }
        return node;
    }

    public static ArrayNode ofStrings(List<String> values) {
        ArrayNode array = array();
        values.forEach(array::add);
        return array;
    }

    public static ObjectNode ofMap(Map<String, String> values) {
        ObjectNode node = object();
        values.forEach(node::put);
        return node;
    }

    private Json() {}
}
