package cn.syntomic.qflink.common.codec.deser;

import java.io.IOException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.ClassUtil;

/**
 * Nested List deserializer, similar to {@link CollectionDeserializer}
 *
 * @version base 2.13.2
 */
public class NestedListDeserializer extends JsonDeserializer<QArrayList> {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().setSerializationInclusion(Include.NON_NULL);

    @Override
    public QArrayList deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        QArrayList result = new QArrayList();
        return deserialize(p, ctx, result);
    }

    @Override
    public QArrayList deserialize(JsonParser p, DeserializationContext ctx, QArrayList result)
            throws IOException {
        // [databind#631]: Assign current value, to be accessible by custom serializers
        p.setCurrentValue(result);

        JsonDeserializer<Object> valueDes = UntypedObjectDeserializer.Vanilla.std;

        JsonToken t;
        while ((t = p.nextToken()) != JsonToken.END_ARRAY) {
            try {
                Object value;
                if (t == JsonToken.VALUE_NULL) {
                    value = valueDes.getNullValue(ctx);
                } else {
                    value = valueDes.deserialize(p, ctx);
                }

                if (value instanceof String) {
                    result.add((String) value);
                } else if (value != null) {
                    result.add(MAPPER.writeValueAsString(value));
                }
            } catch (Exception e) {
                boolean wrap =
                        (ctx == null) || ctx.isEnabled(DeserializationFeature.WRAP_EXCEPTIONS);
                if (!wrap) {
                    ClassUtil.throwIfRTE(e);
                }
                throw JsonMappingException.wrapWithPath(e, result, result.size());
            }
        }
        return result;
    }
}
