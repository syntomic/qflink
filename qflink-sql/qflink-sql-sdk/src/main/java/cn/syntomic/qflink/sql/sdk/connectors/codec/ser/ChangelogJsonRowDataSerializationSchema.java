package cn.syntomic.qflink.sql.sdk.connectors.codec.ser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Serialization schema from Flink Table/SQL internal data structure {@link RowData} to JSON with
 * the row kind.
 *
 * <p>The row kind is written as the {@code row_kind} field, the same field {@link
 * cn.syntomic.qflink.sql.sdk.connectors.codec.deser.ChangelogJsonRowDataDeserializationSchema}
 * reads, so a changelog survives a round trip.
 */
public class ChangelogJsonRowDataSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final String CHANGELOG_FIELD_NAME = "row_kind";

    private final RowType physicalRowType;

    /** Serializer of the physical fields plus the row kind field. */
    private final JsonRowDataSerializationSchema jsonSerializer;

    private final RowData.FieldGetter[] fieldGetters;

    private transient GenericRowData reuse;

    public ChangelogJsonRowDataSerializationSchema(
            RowType physicalRowType,
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber,
            boolean ignoreNullFields) {

        this.physicalRowType = physicalRowType;
        this.jsonSerializer =
                new JsonRowDataSerializationSchema(
                        createJsonRowType(physicalRowType),
                        timestampFormat,
                        mapNullKeyMode,
                        mapNullKeyLiteral,
                        encodeDecimalAsPlainNumber,
                        ignoreNullFields);

        this.fieldGetters = new RowData.FieldGetter[physicalRowType.getFieldCount()];
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(physicalRowType.getTypeAt(i), i);
        }
    }

    /** The row kind comes first, like in the changelog files read by the qfile source. */
    private static RowType createJsonRowType(RowType physicalRowType) {
        List<RowField> fields = new ArrayList<>(physicalRowType.getFieldCount() + 1);
        fields.add(new RowField(CHANGELOG_FIELD_NAME, new VarCharType(VarCharType.MAX_LENGTH)));
        fields.addAll(physicalRowType.getFields());

        return new RowType(false, fields);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonSerializer.open(context);
        reuse = new GenericRowData(fieldGetters.length + 1);
    }

    @Override
    public byte[] serialize(RowData row) {
        // ! +I, -U, +U or -D, the same encoding the deserialization schema expects
        reuse.setField(0, StringData.fromString(row.getRowKind().shortString()));
        for (int i = 0; i < fieldGetters.length; i++) {
            reuse.setField(i + 1, fieldGetters[i].getFieldOrNull(row));
        }

        return jsonSerializer.serialize(reuse);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChangelogJsonRowDataSerializationSchema that = (ChangelogJsonRowDataSerializationSchema) o;
        return Objects.equals(physicalRowType, that.physicalRowType)
                && Objects.equals(jsonSerializer, that.jsonSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalRowType, jsonSerializer);
    }
}
