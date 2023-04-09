package cn.syntomic.qflink.common.codec.deser;

import java.io.IOException;
import java.util.Arrays;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StringDeserializer;

/** Clean dirty strings to null when deserializer */
public class StringCleanDeserializer extends StdDeserializer<String> {

    /** Dirty strings to be cleaned */
    public final String[] cleanStrings;

    public StringCleanDeserializer(String[] cleanStrings) {
        super(String.class);
        this.cleanStrings = cleanStrings;
    }

    @Override
    public String deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        String result = StringDeserializer.instance.deserialize(p, ctx);
        if (Arrays.stream(cleanStrings).anyMatch(result::equalsIgnoreCase)) {
            return null;
        }
        return result;
    }
}
