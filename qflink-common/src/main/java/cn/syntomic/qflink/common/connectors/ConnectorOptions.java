package cn.syntomic.qflink.common.connectors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ConnectorOptions {

    public static final String DEFAULT_SOURCE = "qfile";
    public static final String DEFAULT_SINK = "print";

    public static final ConfigOption<String> SOURCE =
            ConfigOptions.key("source")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Source type, eg qfile, kafka and so on.");

    public static final ConfigOption<String> SOURCE_NAME =
            ConfigOptions.key("source.name")
                    .stringType()
                    .defaultValue("custom-source")
                    .withDescription("Source name.");

    public static final ConfigOption<Integer> SOURCE_PARALLELISM =
            ConfigOptions.key("source.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Source Parallelism, default is the global parallelism.");

    public static final ConfigOption<String> SINK =
            ConfigOptions.key("sink")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Sink type, eg print, kafka and so on.");

    public static final ConfigOption<String> SINK_NAME =
            ConfigOptions.key("sink.name")
                    .stringType()
                    .defaultValue("Unnamed")
                    .withDescription("Sink name.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Sink Parallelism, default is the global parallelism.");

    private ConnectorOptions() {}
}
