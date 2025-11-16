package cn.syntomic.qflink.sql.sdk.connectors.sinks.qprint;

import static org.apache.flink.connector.print.table.PrintConnectorOptions.PRINT_IDENTIFIER;
import static org.apache.flink.connector.print.table.PrintConnectorOptions.STANDARD_ERROR;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
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
            staticPartitions.forEach(
                    (key, value) -> {
                        printIdentifier = null != printIdentifier ? printIdentifier + ":" : "";
                        printIdentifier += key + "=" + value;
                    });

            return SinkV2Provider.of(new RowDataPrintSink(IDENTIFIER, stdErr), parallelism);
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

    private static class RowDataPrintSink implements Sink<RowData> {

        private static final long serialVersionUID = 1L;
        private final String sinkIdentifier;
        private final boolean isStdErr;

        /**
         * Instantiates a print sink that prints to STDOUT or STDERR and gives a sink identifier.
         *
         * @param sinkIdentifier Message that identifies the sink and is prefixed to the output of
         *     the value
         * @param isStdErr True if the sink should print to STDERR instead of STDOUT.
         */
        public RowDataPrintSink(final String sinkIdentifier, final boolean isStdErr) {
            this.sinkIdentifier = sinkIdentifier;
            this.isStdErr = isStdErr;
        }

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
            final PrintSinkOutputWriter<RowData> writer =
                    new PrintSinkOutputWriter<>(sinkIdentifier, isStdErr);
            writer.open(
                    context.getTaskInfo().getIndexOfThisSubtask(),
                    context.getTaskInfo().getNumberOfParallelSubtasks());
            return writer;
        }

        @Override
        public String toString() {
            return "Print to " + (isStdErr ? "System.err" : "System.out");
        }
    }
}
