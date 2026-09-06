package cn.syntomic.qflink.sql.sdk.connectors.sources.qfile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

public class QFileTableSourceTest {

    @Test
    void testScanOnceSkipsBlankLines() throws Exception {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnv.executeSql(
                "CREATE TABLE src (a INT) WITH ("
                        + " 'connector' = 'qfile',"
                        + " 'path' = 'blank-line.txt',"
                        + " 'scan-type' = 'once',"
                        + " 'scan-interval' = '0 ms',"
                        + " 'format' = 'json')");

        List<Integer> result = new ArrayList<>();
        try (CloseableIterator<Row> rows = tableEnv.executeSql("SELECT a FROM src").collect()) {
            rows.forEachRemaining(row -> result.add(row.getFieldAs(0)));
        }

        // ! a blank line in the middle of the file must not end the input
        assertEquals(Arrays.asList(1, 2, 3), result);
    }
}
