package cn.syntomic.qflink.sql.sdk.connectors.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

public class ChangelogJsonFormatTest {

    @Test
    void testRowKind() throws Exception {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnv.executeSql(
                "CREATE TABLE src (key1 INT, key2 STRING) WITH ("
                        + " 'connector' = 'qfile',"
                        + " 'path' = 'changelog-data.txt',"
                        + " 'scan-type' = 'once',"
                        + " 'scan-interval' = '0 ms',"
                        + " 'format' = 'changelog-json')");

        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> rows =
                tableEnv.executeSql("SELECT key1, key2 FROM src").collect()) {
            rows.forEachRemaining(row -> result.add(row.toString()));
        }

        assertEquals(
                Arrays.asList("+I[10, val1]", "-U[10, val1]", "+I[2, val1]", "+U[15, val1]"),
                result);
    }

    @Test
    void testUpdatingSink() {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnv.executeSql(
                "CREATE TABLE src (key1 INT, key2 STRING) WITH ("
                        + " 'connector' = 'qfile',"
                        + " 'path' = 'changelog-data.txt',"
                        + " 'scan-type' = 'once',"
                        + " 'format' = 'changelog-json')");

        tableEnv.executeSql(
                "CREATE TABLE sink (key2 STRING, cnt BIGINT) WITH ("
                        + " 'connector' = 'kafka',"
                        + " 'topic' = 'sink-topic',"
                        + " 'properties.bootstrap.servers' = 'localhost:9092',"
                        + " 'format' = 'changelog-json')");

        // ! an updating query is only accepted because the format encodes the row kind
        String plan =
                tableEnv.explainSql(
                        "INSERT INTO sink SELECT key2, COUNT(*) FROM src GROUP BY key2");

        assertTrue(plan.contains("GroupAggregate"), plan);
    }
}
