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
                    .defaultValue(true)
                    .withDescription(
                            "Whether to submit the statement set by TableEnvironment. Set it to"
                                    + " false when the sqls are part of a DataStream pipeline which is"
                                    + " submitted by env.execute().");

    private SQLOptions() {}
}
