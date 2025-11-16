package cn.syntomic.qflink.sql.sdk.connectors.codec;

import static org.apache.flink.formats.json.JsonFormatOptions.DECODE_JSON_PARSER_ENABLED;
import static org.apache.flink.formats.json.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.formats.json.JsonFormatOptions.ENCODE_IGNORE_NULL_FIELDS;
import static org.apache.flink.formats.json.JsonFormatOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_LITERAL;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_MODE;
import static org.apache.flink.formats.json.JsonFormatOptions.TIMESTAMP_FORMAT;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import cn.syntomic.qflink.sql.sdk.connectors.codec.deser.ChangelogJsonRowDataDeserializationSchema;

/**
 * Table format factory for providing configured instances of JSON to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
public class ChangelogJsonFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "changelog-json";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        JsonFormatOptionsUtil.validateDecodingFormatOptions(formatOptions);

        final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        TimestampFormat timestampOption = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);

        return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType physicalDataType,
                    int[][] projections) {
                final DataType producedDataType =
                        Projection.of(projections).project(physicalDataType);
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);

                return new ChangelogJsonRowDataDeserializationSchema(
                        rowType,
                        rowDataTypeInfo,
                        failOnMissingField,
                        ignoreParseErrors,
                        timestampOption);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.all();
            }

            @Override
            public boolean supportsNestedProjection() {
                return false;
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        JsonFormatOptionsUtil.validateEncodingFormatOptions(formatOptions);

        TimestampFormat timestampOption = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                JsonFormatOptionsUtil.getMapNullKeyMode(formatOptions);
        String mapNullKeyLiteral = formatOptions.get(MAP_NULL_KEY_LITERAL);

        final boolean encodeDecimalAsPlainNumber =
                formatOptions.get(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        final boolean ignoreNullFields = formatOptions.get(ENCODE_IGNORE_NULL_FIELDS);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new JsonRowDataSerializationSchema(
                        rowType,
                        timestampOption,
                        mapNullKeyMode,
                        mapNullKeyLiteral,
                        encodeDecimalAsPlainNumber,
                        ignoreNullFields);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        options.add(MAP_NULL_KEY_MODE);
        options.add(MAP_NULL_KEY_LITERAL);
        options.add(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        options.add(ENCODE_IGNORE_NULL_FIELDS);
        options.add(DECODE_JSON_PARSER_ENABLED);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TIMESTAMP_FORMAT);
        options.add(MAP_NULL_KEY_MODE);
        options.add(MAP_NULL_KEY_LITERAL);
        options.add(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        options.add(ENCODE_IGNORE_NULL_FIELDS);
        options.add(DECODE_JSON_PARSER_ENABLED);
        return options;
    }
}
