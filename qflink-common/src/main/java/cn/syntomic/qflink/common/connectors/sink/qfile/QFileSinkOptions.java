package cn.syntomic.qflink.common.connectors.sink.qfile;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Fallback to kafka options in order to not change keys in table api */
public class QFileSinkOptions {

    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(TOPIC.key())
                    .withDescription("File path.");

    public static final ConfigOption<Boolean> TRUNCATE =
            ConfigOptions.key("sink-truncate")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to empty the file when the job starts.");

    private QFileSinkOptions() {}
}
