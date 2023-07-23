package cn.syntomic.qflink.rule.operators;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.rule.configuration.RuleEngineOptions;
import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.entity.Rule.RuleState;

public class LocalCoWindowAggOperatorTest {

    static TwoInputStreamOperatorTestHarness<Rule, Row, Row> testHarness;

    @BeforeAll
    static void setUpTestHarness() throws Exception {
        testHarness = new TwoInputStreamOperatorTestHarness<>(new LocalCoWindowAggOperator());

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        Configuration conf = new Configuration();
        conf.setString(RuleEngineOptions.JOB_ID, "test");
        testHarness.getExecutionConfig().setGlobalJobParameters(conf);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.setup();
        testHarness.open();
    }

    @Test
    void testProcessElement2() throws Exception {

        Rule rule = new Rule();
        rule.setJobId("test");
        rule.setRuleState(RuleState.ACTIVE);
        rule.setRuleId(1);
        rule.setKeys(new String[] {"key"});

        LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>(1);
        positionByName.put("key", 0);
        Row row1 =
                RowUtils.createRowWithNamedPositions(
                        RowKind.INSERT, new Object[] {"key1"}, positionByName);
        Row row2 =
                RowUtils.createRowWithNamedPositions(
                        RowKind.INSERT, new Object[] {"key2"}, positionByName);

        testHarness.processElement1(rule, 0L);
        testHarness.processElement2(row1, 0L);
        testHarness.processElement2(row2, 1L);

        assertThat(testHarness.getOutput())
                .contains(new StreamRecord<>(Row.of(1, "key1", "key1"), 0L))
                .contains(new StreamRecord<>(Row.of(1, "key2", "key2"), 1L));
    }
}
