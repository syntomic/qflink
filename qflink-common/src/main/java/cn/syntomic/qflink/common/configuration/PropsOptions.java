package cn.syntomic.qflink.common.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class PropsOptions {

    public static final ConfigOption<Env> ENV =
            ConfigOptions.key("env")
                    .enumType(Env.class)
                    .defaultValue(Env.DEVELOP)
                    .withDescription("Execute Environment: develop, test, produce.");

    public static final ConfigOption<String> PROPS_FORMAT_DIR =
            ConfigOptions.key("props.format-dir")
                    .stringType()
                    .defaultValue("configs-%s")
                    .withDescription("Job properties relative directory, format with env.");

    public static final ConfigOption<String> PROPS_DEFAULT =
            ConfigOptions.key("props.default-file")
                    .stringType()
                    .defaultValue("job.properties")
                    .withDescription("Default job properties in relative path.");

    public static final ConfigOption<String> PROPS_FILE =
            ConfigOptions.key("props.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specific job properties file in relative path.");

    public static final ConfigOption<String> PROPS_READ_TYPE =
            ConfigOptions.key("props.read-type")
                    .stringType()
                    .defaultValue("resource")
                    .withDescription("Specific job properties file read type.");

    public enum Env {
        DEVELOP,
        TEST,
        PRODUCE
    }
}
