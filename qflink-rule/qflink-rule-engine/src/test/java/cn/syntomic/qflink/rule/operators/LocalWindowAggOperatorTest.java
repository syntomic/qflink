package cn.syntomic.qflink.rule.operators;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

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
import cn.syntomic.qflink.rule.functions.aggregation.DynamicAggregateFunction;
import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class LocalWindowAggOperatorTest {

    private static final long MINUTE = 60 * 1000L;

    /** The elements of a key and a slice cross the shuffle as one partial result. */
    @Test
    void testPartialResultPerSlice() throws Exception {
        Rule rule = cumulateRule(Trigger.BATCH);

        try (TwoInputStreamOperatorTestHarness<Rule, Row, Row> local = openLocal()) {
            local.processElement1(rule, 0);

            local.processElement2(row(1, "key1", "val1"), 0L);
            local.processElement2(row(1, "key1", "val2"), 30_000L);
            local.processElement2(row(1, "key2", "val3"), 30_000L);
            local.processElement2(row(1, "key1", "val4"), 90_000L);

            // ! nothing crosses the network before the buffer is flushed
            assertThat(partials(local)).isEmpty();

            local.processWatermark1(new Watermark(2 * MINUTE));
            local.processWatermark2(new Watermark(2 * MINUTE));

            // one partial result per key and slice, four elements became three records
            assertThat(partials(local))
                    .containsExactlyInAnyOrder(
                            Tuple2.of("1#key1#60000", 2.0),
                            Tuple2.of("1#key2#60000", 1.0),
                            Tuple2.of("1#key1#120000", 1.0));
        }
    }

    /** The two phase aggregation computes the same windows as the single phase one. */
    @Test
    void testTwoPhaseSameAsSinglePhase() throws Exception {
        Rule rule = cumulateRule(Trigger.BATCH);

        assertThat(runTwoPhase(rule, false)).isEqualTo(runSinglePhase(rule));
    }

    /** The second phase can buffer the partial results as well. */
    @Test
    void testTwoPhaseWithMiniBatch() throws Exception {
        Rule rule = cumulateRule(Trigger.BATCH);

        assertThat(runTwoPhase(rule, true)).isEqualTo(runSinglePhase(rule));
    }

    /** The buffered elements must not resurrect the state of a cleared rule. */
    @Test
    void testClearStateDropsBuffer() throws Exception {
        Rule rule = cumulateRule(Trigger.BATCH);

        try (TwoInputStreamOperatorTestHarness<Rule, Row, Row> local = openLocal()) {
            local.processElement1(rule, 0);
            local.processElement2(row(1, "key1", "val1"), 0L);

            Rule clear = cumulateRule(Trigger.BATCH);
            clear.setClearState(true);
            local.processElement1(clear, 0);

            local.processWatermark1(new Watermark(2 * MINUTE));
            local.processWatermark2(new Watermark(2 * MINUTE));

            assertThat(partials(local)).isEmpty();
        }
    }

    // --------------------------------------------------------------------

    /** Feed the elements to the aggregate operator directly. */
    private List<Tuple2<String, Double>> runSinglePhase(Rule rule) throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, String>, Rule, Row, Row>
                global = openGlobal(false, false)) {
            global.processElement1(rule, 0);
            global.processBothWatermarks(new Watermark(-1L));

            for (StreamRecord<Row> element : elements()) {
                global.processElement2(element);
            }

            global.processBothWatermarks(new Watermark(4 * MINUTE));
            return alerts(global);
        }
    }

    /** Aggregate the slices first and only shuffle the accumulators. */
    private List<Tuple2<String, Double>> runTwoPhase(Rule rule, boolean miniBatch)
            throws Exception {
        List<StreamRecord<Row>> partials = new ArrayList<>();

        try (TwoInputStreamOperatorTestHarness<Rule, Row, Row> local = openLocal()) {
            local.processElement1(rule, 0);

            for (StreamRecord<Row> element : elements()) {
                local.processElement2(element);
            }

            local.processWatermark1(new Watermark(4 * MINUTE));
            local.processWatermark2(new Watermark(4 * MINUTE));

            for (Object record : local.getOutput()) {
                if (record instanceof StreamRecord) {
                    @SuppressWarnings("unchecked")
                    StreamRecord<Row> partial = (StreamRecord<Row>) record;
                    partials.add(partial);
                }
            }
        }

        try (KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, String>, Rule, Row, Row>
                global = openGlobal(true, miniBatch)) {
            global.processElement1(rule, 0);
            global.processBothWatermarks(new Watermark(-1L));

            for (StreamRecord<Row> partial : partials) {
                global.processElement2(partial);
            }

            global.processBothWatermarks(new Watermark(4 * MINUTE));
            return alerts(global);
        }
    }

    private TwoInputStreamOperatorTestHarness<Rule, Row, Row> openLocal() throws Exception {
        TwoInputStreamOperatorTestHarness<Rule, Row, Row> harness =
                new TwoInputStreamOperatorTestHarness<>(new LocalWindowAggOperator());

        Configuration conf = new Configuration();
        conf.set(RuleEngineOptions.JOB_ID, "test");
        conf.set(RuleEngineOptions.AGG_MINI_BATCH_SIZE, 100L);
        conf.set(RuleEngineOptions.AGG_MINI_BATCH_ALLOW_LATENCY, Duration.ofSeconds(10));
        harness.getExecutionConfig().setGlobalJobParameters(conf);

        harness.setup();
        harness.open();
        return harness;
    }

    private KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, String>, Rule, Row, Row>
            openGlobal(boolean localAggEnabled, boolean miniBatch) throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, String>, Rule, Row, Row> harness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new GlobalCoWindowAggOperator(),
                        null,
                        (Row value) -> Tuple2.of(value.getFieldAs(0), value.getFieldAs(1)),
                        Types.TUPLE(Types.INT, Types.STRING));

        Configuration conf = new Configuration();
        conf.set(RuleEngineOptions.JOB_ID, "test");
        conf.set(RuleEngineOptions.AGG_WINDOW_ALLOW_LATENESS, Duration.ZERO);
        conf.set(RuleEngineOptions.AGG_LOCAL_ENABLED, localAggEnabled);
        conf.set(RuleEngineOptions.AGG_MINI_BATCH_ENABLED, miniBatch);
        conf.set(RuleEngineOptions.AGG_MINI_BATCH_SIZE, 100L);
        conf.set(RuleEngineOptions.AGG_MINI_BATCH_ALLOW_LATENCY, Duration.ofSeconds(10));
        harness.getExecutionConfig().setGlobalJobParameters(conf);

        harness.setup();
        harness.open();
        return harness;
    }

    private static List<StreamRecord<Row>> elements() {
        List<StreamRecord<Row>> elements = new ArrayList<>();
        elements.add(new StreamRecord<>(row(1, "key1", "val1"), 0L));
        elements.add(new StreamRecord<>(row(1, "key1", "val2"), 30_000L));
        elements.add(new StreamRecord<>(row(1, "key2", "val3"), 30_000L));
        elements.add(new StreamRecord<>(row(1, "key1", "val4"), 90_000L));
        elements.add(new StreamRecord<>(row(1, "key1", "val5"), 150_000L));
        return elements;
    }

    /** The key, the slice and the count of every partial result. */
    private static List<Tuple2<String, Double>> partials(
            TwoInputStreamOperatorTestHarness<Rule, Row, Row> harness) {
        return harness.getOutput().stream()
                .filter(StreamRecord.class::isInstance)
                .map(record -> (Row) ((StreamRecord<?>) record).getValue())
                .map(
                        row ->
                                Tuple2.of(
                                        row.getField(0)
                                                + "#"
                                                + row.getField(1)
                                                + "#"
                                                + row.getField(2),
                                        DynamicAggregateFunction.toResult(
                                                        row.<Map<String, QAccumulator>>getFieldAs(
                                                                3))
                                                .get("val_cnt")))
                .collect(Collectors.toList());
    }

    /** The alert time and the metric of every alert. */
    private static List<Tuple2<String, Double>> alerts(
            KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, String>, Rule, Row, Row>
                    harness) {
        return harness.getOutput().stream()
                .filter(StreamRecord.class::isInstance)
                .map(record -> (Row) ((StreamRecord<?>) record).getValue())
                .map(
                        row ->
                                Tuple2.of(
                                        row.<Map<String, String>>getFieldAs(2).get("key")
                                                + "@"
                                                + row.<String>getFieldAs(1),
                                        row.<Map<String, Double>>getFieldAs(3).get("val_cnt")))
                .sorted((left, right) -> left.f0.compareTo(right.f0))
                .collect(Collectors.toList());
    }

    private static Rule cumulateRule(Trigger trigger) {
        Rule rule = new Rule();
        rule.setJobId("test");
        rule.setRuleId(1);
        rule.setKeys(new String[] {"key"});
        rule.setRuleState(RuleState.ACTIVE);
        rule.setWindow(new WindowRule(WindowType.CUMULATE, trigger, 4 * MINUTE, 0L, MINUTE));
        rule.setAggregates(
                new Aggregate[] {
                    new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null)
                });
        rule.setThreshold(new Eval("val_cnt>0", new String[] {"val_cnt"}));
        return rule;
    }

    private static Row row(int ruleId, String key, String value) {
        LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>(3);
        positionByName.put("rule_id", 0);
        positionByName.put("key", 1);
        positionByName.put("val", 2);
        return RowUtils.createRowWithNamedPositions(
                RowKind.INSERT, new Object[] {ruleId, key, value}, positionByName);
    }

    static {
        // the time zone of the alert time must not change the comparison
        TimeUtil.longToString(0L);
    }
}
