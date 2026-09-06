package cn.syntomic.qflink.sql.sdk.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SQLExecutorTest {

    private StreamTableEnvironment tableEnv;

    @BeforeEach
    void setUp() {
        tableEnv =
                StreamTableEnvironment.create(StreamExecutionEnvironment.createLocalEnvironment(1));
    }

    @Test
    void testSetKeepsSpecialCharacters() {
        // no insert statement, nothing is submitted
        SQLExecutor.of(tableEnv)
                .execute(
                        new String[] {
                            "SET 'pipeline.name' = 'ASSET LIST'",
                            "SET 'a.key' = 'a=b=c'",
                            "SET \"quoted.key\" = \"value\""
                        },
                        false);

        assertEquals("ASSET LIST", get("pipeline.name"));
        assertEquals("a=b=c", get("a.key"));
        assertEquals("value", get("quoted.key"));
    }

    @Test
    void testInvalidSetIsIgnored() {
        SQLExecutor.of(tableEnv).execute(new String[] {"SET 'only.key'"}, false);

        assertNull(get("only.key"));
    }

    private String get(String key) {
        ConfigOption<String> option = ConfigOptions.key(key).stringType().noDefaultValue();
        return tableEnv.getConfig().getConfiguration().get(option);
    }
}
