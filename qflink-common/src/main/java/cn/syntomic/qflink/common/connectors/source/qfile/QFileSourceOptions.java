package cn.syntomic.qflink.common.connectors.source.qfile;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;

import java.time.Duration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Fallback to kafka options in order to not change keys in table api */
public class QFileSourceOptions {

    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(TOPIC.key())
                    .withDescription("File path.");

    public static final ConfigOption<String> SCAN_TYPE =
            ConfigOptions.key("scan-type")
                    .stringType()
                    .defaultValue("cont")
                    .withFallbackKeys(SCAN_STARTUP_MODE.key())
                    .withDescription("File scan type: cont, rand, once.");

    public static final ConfigOption<Duration> INTERVAL =
            ConfigOptions.key("scan-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(2L))
                    .withFallbackKeys(SCAN_TOPIC_PARTITION_DISCOVERY.key())
                    .withDescription("File scan interval.");

    public static final ConfigOption<Duration> PAUSE =
            ConfigOptions.key("scan-pause")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0L))
                    .withFallbackKeys(SCAN_STARTUP_TIMESTAMP_MILLIS.key())
                    .withDescription("File read after pause duration.");

    private QFileSourceOptions() {}
}
