package cn.syntomic.qflink.common.codec.deser;

import java.io.IOException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;

/**
 * Nested map deserializer, similar to {@link MapDeserializer}
 *
 * @version base 2.13.2
 */
public class NestedMapDeserializer extends JsonDeserializer<QLinkedHashMap> {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().setSerializationInclusion(Include.NON_NULL);

    @SuppressWarnings("deprecation")
    @Override
    public QLinkedHashMap deserialize(JsonParser p, DeserializationContext ctx) throws IOException {

        // similar to Map<String, Object>
        JsonDeserializer<Object> valueDes = UntypedObjectDeserializer.Vanilla.std;
        QLinkedHashMap result = new QLinkedHashMap();
        String key;
        if (p.isExpectedStartObjectToken()) {
            key = p.nextFieldName();
        } else {
            JsonToken t = p.currentToken();
            if (t == JsonToken.END_OBJECT) {
                return result;
            }
            if (t != JsonToken.FIELD_NAME) {
                ctx.reportWrongTokenException(this, JsonToken.FIELD_NAME, null);
            }
            key = p.currentName();
        }

        for (; key != null; key = p.nextFieldName()) {
            JsonToken t = p.nextToken();
            try {
                // Note: must handle null explicitly here; value deserializers won't
                Object value;
                if (t == JsonToken.VALUE_NULL) {
                    value = valueDes.getNullValue(ctx);
                } else {
                    value = valueDes.deserialize(p, ctx);
                }

                if (value instanceof String) {
                    // avoid quotes
                    result.put(key, (String) value);
                } else if (value != null) {
                    // ignore null
                    result.put(key, MAPPER.writeValueAsString(value));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
