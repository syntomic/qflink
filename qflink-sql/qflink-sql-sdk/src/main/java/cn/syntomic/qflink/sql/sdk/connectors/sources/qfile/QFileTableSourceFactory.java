package cn.syntomic.qflink.sql.sdk.connectors.sources.qfile;

import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.INTERVAL;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.PATH;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.PAUSE;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.SCAN_TYPE;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class QFileTableSourceFactory implements DynamicTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "qfile";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        options.add(FactoryUtil.FORMAT);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_TYPE);
        options.add(INTERVAL);
        options.add(PAUSE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper factoryHelper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                factoryHelper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new QFileTableSource(
                decodingFormat,
                producedDataType,
                factoryHelper.getOptions().get(PATH),
                factoryHelper.getOptions().get(SCAN_TYPE),
                factoryHelper.getOptions().get(INTERVAL).toMillis(),
                factoryHelper.getOptions().get(PAUSE).toMillis());
    }
}
