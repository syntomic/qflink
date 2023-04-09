package cn.syntomic.qflink.common.connectors.helper.kafka;

import java.util.Collections;
import java.util.Map;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class KafkaOptions {

    public static final ConfigOption<String> SASL_USERNAME =
            ConfigOptions.key("sasl.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SASL configurations of username.");

    public static final ConfigOption<String> SASL_PASSWORD =
            ConfigOptions.key("sasl.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SASL configurations of password.");

    public static final ConfigOption<Map<String, String>> KAFKA_PROPERTIES =
            ConfigOptions.key("kafka.properties")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("This can set and pass arbitrary Kafka configurations.");

    private KafkaOptions() {}
}
