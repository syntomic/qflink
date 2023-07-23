package cn.syntomic.qflink.rule.operators;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.common.utils.TimeUtil;
import cn.syntomic.qflink.rule.configuration.RuleEngineOptions;
import cn.syntomic.qflink.rule.entity.Aggregate;
import cn.syntomic.qflink.rule.entity.Aggregate.Method;
import cn.syntomic.qflink.rule.entity.Eval;
import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.entity.Rule.RuleState;
import cn.syntomic.qflink.rule.entity.WindowRule;
import cn.syntomic.qflink.rule.entity.WindowRule.Trigger;
import cn.syntomic.qflink.rule.entity.WindowRule.WindowType;
import cn.syntomic.qflink.rule.utils.RowUtil;

public class GlobalCoWindowAggOperatorTest {

    static KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, String>, Rule, Row, Row>
            testHarness;

    @BeforeAll
    static void setUpTestHarness() throws Exception {

        testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new GlobalCoWindowAggOperator(),
                        null,
                        (Row value) -> Tuple2.of(value.getFieldAs(0), value.getFieldAs(1)),
                        Types.TUPLE(Types.INT, Types.STRING));

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
        rule.setRuleId(1);
        rule.setKeys(new String[] {"key"});
        rule.setRuleState(RuleState.ACTIVE);
        rule.setWindow(new WindowRule(WindowType.TUMBLE, Trigger.SINGLE, 60000L, 0L, 0L));
        rule.setAggregates(
                new Aggregate[] {
                    new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null)
                });
        rule.setThreshold(new Eval("val_cnt>1", new String[] {"val_cnt"}));

        LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>(3);
        positionByName.put("rule_id", 0);
        positionByName.put("key", 1);
        positionByName.put("val", 2);
        Row row1 =
                RowUtils.createRowWithNamedPositions(
                        RowKind.INSERT, new Object[] {1, "key1", "val1"}, positionByName);
        Row row2 =
                RowUtils.createRowWithNamedPositions(
                        RowKind.INSERT, new Object[] {1, "key1", "val2"}, positionByName);

        testHarness.processElement1(rule, 0);

        testHarness.processBothWatermarks(new Watermark(0L));
        testHarness.processElement2(row1, 0L);
        testHarness.processElement2(row2, 30000L);
        testHarness.processBothWatermarks(new Watermark(200000L));

        StreamRecord<Row> expectedOutput =
                new StreamRecord<>(
                        Row.of(
                                1,
                                TimeUtil.longToString(30000L),
                                Map.of("key", "key1"),
                                Map.of("val_cnt", 2.0),
                                RowUtil.toStringMap(row2)),
                        30000L);

        assertThat(testHarness.getOutput()).contains(expectedOutput);
    }
}
