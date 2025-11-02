package cn.syntomic.qflink.sql.sdk.connectors.sinks.qprint;

import static org.apache.flink.connector.print.table.PrintConnectorOptions.PRINT_IDENTIFIER;
import static org.apache.flink.connector.print.table.PrintConnectorOptions.STANDARD_ERROR;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.legacy.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

/** Print without validate options */
public class QPrintTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "qprint";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PRINT_IDENTIFIER);
        options.add(STANDARD_ERROR);
        options.add(FactoryUtil.SINK_PARALLELISM);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        return new PrintSink(
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType(),
                context.getCatalogTable().getPartitionKeys(),
                options.get(PRINT_IDENTIFIER),
                options.get(STANDARD_ERROR),
                options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    private static class PrintSink implements DynamicTableSink, SupportsPartitioning {

        private final DataType type;
        private String printIdentifier;
        private final boolean stdErr;
        private final @Nullable Integer parallelism;
        private final List<String> partitionKeys;
        private Map<String, String> staticPartitions = new LinkedHashMap<>();

        private PrintSink(
                DataType type,
                List<String> partitionKeys,
                String printIdentifier,
                boolean stdErr,
                Integer parallelism) {
            this.type = type;
            this.partitionKeys = partitionKeys;
            this.printIdentifier = printIdentifier;
            this.stdErr = stdErr;
            this.parallelism = parallelism;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(DynamicTableSink.Context context) {
            DataStructureConverter converter = context.createDataStructureConverter(type);
            staticPartitions.forEach(
                    (key, value) -> {
                        printIdentifier = null != printIdentifier ? printIdentifier + ":" : "";
                        printIdentifier += key + "=" + value;
                    });
            return SinkFunctionProvider.of(
                    new RowDataPrintFunction(converter, printIdentifier, stdErr), parallelism);
        }

        @Override
        public DynamicTableSink copy() {
            return new PrintSink(type, partitionKeys, printIdentifier, stdErr, parallelism);
        }

        @Override
        public String asSummaryString() {
            return "Print to " + (stdErr ? "System.err" : "System.out");
        }

        @Override
        public void applyStaticPartition(Map<String, String> partition) {
            // make it a LinkedHashMap to maintain partition column order
            staticPartitions = new LinkedHashMap<>();
            for (String partitionCol : partitionKeys) {
                if (partition.containsKey(partitionCol)) {
                    staticPartitions.put(partitionCol, partition.get(partitionCol));
                }
            }
        }
    }

    /**
     * Implementation of the SinkFunction converting {@link RowData} to string and passing to {@link
     * PrintSinkFunction}.
     */
    private static class RowDataPrintFunction extends RichSinkFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private final DataStructureConverter converter;
        private final PrintSinkOutputWriter<String> writer;

        private RowDataPrintFunction(
                DataStructureConverter converter, String printIdentifier, boolean stdErr) {
            this.converter = converter;
            this.writer = new PrintSinkOutputWriter<>(printIdentifier, stdErr);
        }

        @Override
        public void open(OpenContext parameters) throws Exception {
            super.open(parameters);
            // TODO open
            // StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            // writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
        }

        @Override
        @SuppressWarnings("null")
        public void invoke(RowData value, Context context) {
            Object data = converter.toExternal(value);
            assert data != null;
            writer.write(data.toString());
        }
    }
}
