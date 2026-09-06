package cn.syntomic.qflink.sql.sdk.connectors.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions.MapNullKeyMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.sql.sdk.connectors.codec.deser.ChangelogJsonRowDataDeserializationSchema;
import cn.syntomic.qflink.sql.sdk.connectors.codec.ser.ChangelogJsonRowDataSerializationSchema;

public class ChangelogJsonSerDeTest {

    private static final RowType ROW_TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("key1", DataTypes.INT()),
                                    DataTypes.FIELD("key2", DataTypes.STRING()))
                            .getLogicalType();

    @Test
    void testSerializeRowKind() throws Exception {
        ChangelogJsonRowDataSerializationSchema serializer = createSerializer();

        assertEquals(
                "{\"row_kind\":\"+I\",\"key1\":10,\"key2\":\"val1\"}",
                serialize(serializer, RowKind.INSERT));
        assertEquals(
                "{\"row_kind\":\"-U\",\"key1\":10,\"key2\":\"val1\"}",
                serialize(serializer, RowKind.UPDATE_BEFORE));
        assertEquals(
                "{\"row_kind\":\"+U\",\"key1\":10,\"key2\":\"val1\"}",
                serialize(serializer, RowKind.UPDATE_AFTER));
        assertEquals(
                "{\"row_kind\":\"-D\",\"key1\":10,\"key2\":\"val1\"}",
                serialize(serializer, RowKind.DELETE));
    }

    @Test
    void testRoundTrip() throws Exception {
        List<String> changelog =
                Arrays.asList(
                        "{\"row_kind\":\"+I\",\"key1\":10,\"key2\":\"val1\"}",
                        "{\"row_kind\":\"-U\",\"key1\":10,\"key2\":\"val1\"}",
                        "{\"row_kind\":\"+U\",\"key1\":15,\"key2\":\"val1\"}",
                        "{\"row_kind\":\"-D\",\"key1\":15,\"key2\":\"val1\"}");

        ChangelogJsonRowDataDeserializationSchema deserializer =
                new ChangelogJsonRowDataDeserializationSchema(
                        ROW_TYPE, InternalTypeInfo.of(ROW_TYPE), false, false, TimestampFormat.SQL);
        deserializer.open(null);
        ChangelogJsonRowDataSerializationSchema serializer = createSerializer();

        List<String> result = new ArrayList<>();
        for (String json : changelog) {
            List<RowData> rows = new ArrayList<>();
            deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8), collector(rows));
            for (RowData row : rows) {
                result.add(new String(serializer.serialize(row), StandardCharsets.UTF_8));
            }
        }

        // the row kind survives a decode and encode round trip
        assertEquals(changelog, result);
    }

    private static ChangelogJsonRowDataSerializationSchema createSerializer() throws Exception {
        ChangelogJsonRowDataSerializationSchema serializer =
                new ChangelogJsonRowDataSerializationSchema(
                        ROW_TYPE, TimestampFormat.SQL, MapNullKeyMode.FAIL, "null", false, false);
        serializer.open(null);
        return serializer;
    }

    private static String serialize(
            ChangelogJsonRowDataSerializationSchema serializer, RowKind rowKind) {
        GenericRowData row = GenericRowData.of(10, StringData.fromString("val1"));
        row.setRowKind(rowKind);
        return new String(serializer.serialize(row), StandardCharsets.UTF_8);
    }

    private static Collector<RowData> collector(List<RowData> rows) {
        return new Collector<RowData>() {
            @Override
            public void collect(RowData record) {
                rows.add(record);
            }

            @Override
            public void close() {
                // nothing to do
            }
        };
    }
}
