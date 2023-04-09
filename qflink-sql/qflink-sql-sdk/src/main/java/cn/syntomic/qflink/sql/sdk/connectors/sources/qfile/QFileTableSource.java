package cn.syntomic.qflink.sql.sdk.connectors.sources.qfile;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceFunction;

public class QFileTableSource implements ScanTableSource {

    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;
    private final String path;
    private final String scanType;
    private final long interval;
    private final long pause;

    public QFileTableSource(
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType,
            String path,
            String scanType,
            long interval,
            long pause) {

        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;

        this.path = path;
        this.scanType = scanType;
        this.interval = interval;
        this.pause = pause;
    }

    @Override
    public DynamicTableSource copy() {
        return new QFileTableSource(
                decodingFormat, producedDataType, path, scanType, interval, pause);
    }

    @Override
    public String asSummaryString() {
        return "QFile Table Source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        final DeserializationSchema<RowData> deserializer =
                decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);

        final SourceFunction<RowData> sourceFunction =
                QFileSourceFunction.of(path, scanType, interval, pause, deserializer);

        return SourceFunctionProvider.of(sourceFunction, false);
    }
}
