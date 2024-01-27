package cn.syntomic.qflink.sql.udf.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.sql.udf.utils.CustomCollector;

public class QRegexExtractTest {
    @Test
    void testEval() throws Exception {

        QRegexExtract udtf = new QRegexExtract();
        CustomCollector collector = new CustomCollector();

        udtf.setCollector(collector);
        udtf.open(null);

        String raw = "2023-04-09 15:40:05 stdout {\"key1\": 1,\"key2\":\"val1\"}";
        String regex = "(.*) (.*) (\\{.*\\})";
        udtf.setPattern(regex);

        udtf.eval(raw, regex, "time", "key_word", "data");

        List<String> result =
                new ArrayList<>(collector.getOutputs())
                        .stream().map(Row::toString).sorted().collect(Collectors.toList());
        List<String> expected = new ArrayList<>();
        expected.add("+I[2023-04-09 15:40:05, stdout, {\"key1\": 1,\"key2\":\"val1\"}]");

        assertEquals(expected, result);
    }
}
