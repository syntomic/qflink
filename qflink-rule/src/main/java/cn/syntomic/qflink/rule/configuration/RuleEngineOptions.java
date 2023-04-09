package cn.syntomic.qflink.rule.configuration;

import java.time.Duration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ETLOutput;
import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.LogFormat;
import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.WatermarkTiming;

public class RuleEngineOptions {

    public static final ConfigOption<String> JOB_ID =
            ConfigOptions.key("job.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A flink job handles the same job_id of rule.");

    public static final ConfigOption<WatermarkTiming> WATERMARK_TIMING =
            ConfigOptions.key("job.watermark-timing")
                    .enumType(WatermarkTiming.class)
                    .defaultValue(WatermarkTiming.WITHOUT)
                    .withDescription("Assign watermark timing.");

    public static final ConfigOption<LogFormat> LOG_FORMAT =
            ConfigOptions.key("log.format")
                    .enumType(LogFormat.class)
                    .defaultValue(LogFormat.JSON)
                    .withDescription("The log source format.");

    public static final ConfigOption<String> LOG_SCHEMA =
            ConfigOptions.key("log.schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Avro schema to decide log schema.");

    public static final ConfigOption<String> LOG_FIELD_TIME =
            ConfigOptions.key("log.field-time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The log's time field name, string of format: yyyy-MM-dd HH:mm:ss.");

    public static final ConfigOption<String> ETL_DEFAULT_TABLE =
            ConfigOptions.key("etl.default-table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The default table to sink.");

    public static final ConfigOption<ETLOutput> ETL_OUTPUT =
            ConfigOptions.key("etl.output")
                    .enumType(ETLOutput.class)
                    .defaultValue(ETLOutput.INHERIT)
                    .withDescription("ETL operator's output type.");

    public static final ConfigOption<String> ETL_OUTPUT_SCHEMA =
            ConfigOptions.key("etl.output-schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The ETL output type info is needed when etl.output==SPECIFIC.");

    public static final ConfigOption<Boolean> AGG_ENABLE =
            ConfigOptions.key("agg.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable agg operator.");

    public static final ConfigOption<Integer> AGG_PARALLELISM =
            ConfigOptions.key("agg.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the aggregate parallelism, default is the global parallelism.");

    public static final ConfigOption<Duration> AGG_WINDOW_ALLOW_LATENESS =
            ConfigOptions.key("agg.window.allow-lateness")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("Specify the maximum allowed latency for window.");

    private RuleEngineOptions() {}
}
