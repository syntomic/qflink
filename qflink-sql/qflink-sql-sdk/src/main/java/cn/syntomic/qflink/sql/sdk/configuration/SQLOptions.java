package cn.syntomic.qflink.sql.sdk.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class SQLOptions {

    public static final ConfigOption<String> EXECUTE_SQL =
            ConfigOptions.key("sql.execute-sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SQL file name.");

    public static final ConfigOption<String> READ_TYPE =
            ConfigOptions.key("sql.read-type")
                    .stringType()
                    .defaultValue("resource")
                    .withDescription("SQL file read type.");

    public static final ConfigOption<Boolean> EXECUTE_ENABLE =
            ConfigOptions.key("sql.execute-enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to execute the job by TableEnvironment.");

    private SQLOptions() {}
}
