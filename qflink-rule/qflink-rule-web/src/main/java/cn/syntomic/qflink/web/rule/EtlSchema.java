package cn.syntomic.qflink.web.rule;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import cn.syntomic.qflink.web.http.Json;

/**
 * The fields the ETL operator emits, read from the avro schema of the job({@code
 * etl.output-schema}).
 *
 * <p>They are the fields a rule may key by, aggregate or threshold on, so the page offers them
 * instead of letting everyone guess the names of the flat map.
 */
public class EtlSchema {

    private final Map<String, String> fields = new LinkedHashMap<>();

    private EtlSchema() {}

    /**
     * Parse an avro record schema, an unreadable one is not fatal: the page then simply has no
     * suggestion to offer.
     *
     * @param avroSchema
     * @return
     */
    public static EtlSchema of(String avroSchema) {
        EtlSchema schema = new EtlSchema();
        if (avroSchema == null || avroSchema.isBlank()) {
            return schema;
        }

        try {
            for (JsonNode field : Json.read(avroSchema).path("fields")) {
                String name = field.path("name").asText();
                if (!name.isBlank()) {
                    schema.fields.put(name, type(field.path("type")));
                }
            }
        } catch (RuntimeException e) {
            return schema;
        }

        return schema;
    }

    /** An avro type is a name, a union of names or a nested definition. */
    private static String type(JsonNode type) {
        if (type.isTextual()) {
            return type.asText();
        }
        if (type.isArray()) {
            List<String> names = new ArrayList<>();
            type.forEach(node -> names.add(type(node)));
            names.remove("null");
            return names.isEmpty() ? "unknown" : String.join("|", names);
        }
        return type.path("type").asText("unknown");
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }

    public boolean contains(String field) {
        return fields.containsKey(field);
    }

    public List<String> names() {
        return new ArrayList<>(fields.keySet());
    }

    public Map<String, String> toMap() {
        return new LinkedHashMap<>(fields);
    }
}
