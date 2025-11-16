package cn.syntomic.qflink.sql.sdk.connectors.codec.deser;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.AbstractJsonDeserializationSchema;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

/**
 * Deserialization schema from JSON to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
public class ChangelogJsonRowDataDeserializationSchema extends AbstractJsonDeserializationSchema {
    private static final long serialVersionUID = 1L;

    private static final String CHANGELOG_FEILD_NAME = "row_kind";

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of Flink SQL internal data
     * structures.
     */
    private final JsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;

    public ChangelogJsonRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        super(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat);
        this.runtimeConverter =
                new JsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                        .createConverter(checkNotNull(rowType));
    }

    @Override
    public void deserialize(@Nullable byte[] message, Collector<RowData> out) throws IOException {
        if (message == null) {
            return;
        }
        try {
            final JsonNode root = deserializeToJsonNode(message);
            RowData result = convertToRowData(root);
            if (result != null) {
                if (root.has(CHANGELOG_FEILD_NAME)) {
                    switch (root.get(CHANGELOG_FEILD_NAME).asText()) {
                        case "+U":
                            result.setRowKind(RowKind.UPDATE_AFTER);
                            break;
                        case "-U":
                            result.setRowKind(RowKind.UPDATE_BEFORE);
                            break;
                        case "-D":
                            result.setRowKind(RowKind.DELETE);
                            break;
                        default:
                            result.setRowKind(RowKind.INSERT);
                            break;
                    }
                }
                out.collect(result);
            }
        } catch (Throwable t) {
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Failed to deserialize JSON '%s'.", new String(message)), t);
            }
        }
    }

    public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    public RowData convertToRowData(JsonNode message) {
        return (RowData) runtimeConverter.convert(message);
    }
}
