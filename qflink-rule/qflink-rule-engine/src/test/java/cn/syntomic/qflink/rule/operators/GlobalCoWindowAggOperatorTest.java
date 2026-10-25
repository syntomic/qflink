package cn.syntomic.qflink.rule.operators;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ERROR_TAG;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
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
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import org.junit.jupiter.api.AfterEach;
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

    private static final long MINUTE = 60 * 1000L;

    private KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, String>, Rule, Row, Row>
            testHarness;

    @AfterEach
    void tearDown() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    @Test
    void testProcessElement2() throws Exception {
        openHarness();

        Rule rule =
                rule(
                        new WindowRule(WindowType.TUMBLE, Trigger.SINGLE, MINUTE, 0L, 0L),
                        new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null));

        Row row1 = row(1, "key1", "val1");
        Row row2 = row(1, "key1", "val2");

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

    /** A cumulate window emits at every step, but an element is only stored in its own step. */
    @Test
    void testCumulate() throws Exception {
        openHarness();

        // 4 steps of one minute
        Rule rule =
                rule(
                        new WindowRule(WindowType.CUMULATE, Trigger.BATCH, 4 * MINUTE, 0L, MINUTE),
                        new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null));

        testHarness.processElement1(rule, 0);
        testHarness.processBothWatermarks(new Watermark(-1L));

        testHarness.processElement2(row(1, "key1", "val1"), 0L);
        testHarness.processElement2(row(1, "key1", "val2"), 30_000L);
        testHarness.processElement2(row(1, "key1", "val3"), 90_000L);

        // ! the three elements only live in the two steps they belong to, the copy based
        // ! implementation would have written them into all the four pending windows
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(2);

        testHarness.processBothWatermarks(new Watermark(4 * MINUTE));

        // the window grows step by step, the elements of the first step are counted in all of them
        assertThat(metrics())
                .containsExactly(
                        Tuple2.of(TimeUtil.longToString(MINUTE), 2.0),
                        Tuple2.of(TimeUtil.longToString(2 * MINUTE), 3.0),
                        Tuple2.of(TimeUtil.longToString(3 * MINUTE), 3.0),
                        Tuple2.of(TimeUtil.longToString(4 * MINUTE), 3.0));

        // the state of the cumulate window is cleaned up once the last step has been fired
        assertThat(testHarness.numKeyedStateEntries()).isZero();
    }

    /** A slide window emits one result per step, an element is shared by the windows it spans. */
    @Test
    void testSlide() throws Exception {
        openHarness();

        // three minutes of history, moving every minute
        Rule rule =
                rule(
                        new WindowRule(WindowType.SLIDE, Trigger.BATCH, 3 * MINUTE, 0L, MINUTE),
                        new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null));

        testHarness.processElement1(rule, 0);
        testHarness.processBothWatermarks(new Watermark(-1L));

        testHarness.processElement2(row(1, "key1", "val1"), 30_000L);
        testHarness.processElement2(row(1, "key1", "val2"), 90_000L);

        // ! one slice per minute, not one window per element
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(2);

        testHarness.processBothWatermarks(new Watermark(5 * MINUTE));

        // every window covers the last three minutes, the elements slide out one after the other
        assertThat(metrics())
                .containsExactly(
                        Tuple2.of(TimeUtil.longToString(MINUTE), 1.0),
                        Tuple2.of(TimeUtil.longToString(2 * MINUTE), 2.0),
                        Tuple2.of(TimeUtil.longToString(3 * MINUTE), 2.0),
                        Tuple2.of(TimeUtil.longToString(4 * MINUTE), 1.0));

        assertThat(testHarness.numKeyedStateEntries()).isZero();
    }

    /** An element of a window which has already been fired and cleaned up is reported. */
    @Test
    void testLateElement() throws Exception {
        openHarness();

        Rule rule =
                rule(
                        new WindowRule(WindowType.TUMBLE, Trigger.BATCH, MINUTE, 0L, 0L),
                        new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null));

        testHarness.processElement1(rule, 0);
        testHarness.processBothWatermarks(new Watermark(5 * MINUTE));
        testHarness.processElement2(row(1, "key1", "val1"), 0L);

        assertThat(testHarness.numKeyedStateEntries()).isZero();
        assertThat(errors()).anyMatch(row -> "Late Data".equals(row.getField(3)));
    }

    /** The mini batch only delays the state access, the window results are the same. */
    @Test
    void testMiniBatchSameResult() throws Exception {
        openHarness(
                conf -> {
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_ENABLED, true);
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_SIZE, 100L);
                    conf.set(
                            RuleEngineOptions.AGG_MINI_BATCH_ALLOW_LATENCY, Duration.ofSeconds(10));
                });

        Rule rule =
                rule(
                        new WindowRule(WindowType.CUMULATE, Trigger.BATCH, 4 * MINUTE, 0L, MINUTE),
                        new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null));

        testHarness.processElement1(rule, 0);
        testHarness.processBothWatermarks(new Watermark(-1L));

        testHarness.processElement2(row(1, "key1", "val1"), 0L);
        testHarness.processElement2(row(1, "key1", "val2"), 30_000L);
        testHarness.processElement2(row(1, "key1", "val3"), 90_000L);

        // ! neither the state nor the timers have been touched yet, everything is in the buffer
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        testHarness.processBothWatermarks(new Watermark(4 * MINUTE));

        // same results as without the mini batch, see testCumulate
        assertThat(metrics())
                .containsExactly(
                        Tuple2.of(TimeUtil.longToString(MINUTE), 2.0),
                        Tuple2.of(TimeUtil.longToString(2 * MINUTE), 3.0),
                        Tuple2.of(TimeUtil.longToString(3 * MINUTE), 3.0),
                        Tuple2.of(TimeUtil.longToString(4 * MINUTE), 3.0));
    }

    /** The buffer is flushed as soon as it holds agg.mini-batch.size elements. */
    @Test
    void testMiniBatchSizeFlush() throws Exception {
        openHarness(
                conf -> {
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_ENABLED, true);
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_SIZE, 2L);
                    conf.set(
                            RuleEngineOptions.AGG_MINI_BATCH_ALLOW_LATENCY, Duration.ofSeconds(10));
                });

        Rule rule =
                rule(
                        new WindowRule(WindowType.TUMBLE, Trigger.SINGLE, MINUTE, 0L, 0L),
                        new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null));

        testHarness.processElement1(rule, 0);
        testHarness.processBothWatermarks(new Watermark(-1L));

        testHarness.processElement2(row(1, "key1", "val1"), 0L);
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        // the second element fills the batch, the slice reaches the state
        testHarness.processElement2(row(1, "key1", "val2"), 10_000L);
        assertThat(testHarness.numKeyedStateEntries()).isPositive();

        // ! one alert per slice and per batch, not one per element
        assertThat(metrics()).containsExactly(Tuple2.of(TimeUtil.longToString(10_000L), 2.0));
    }

    /** The buffer is flushed at the latest after agg.mini-batch.allow-latency. */
    @Test
    void testMiniBatchLatencyFlush() throws Exception {
        openHarness(
                conf -> {
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_ENABLED, true);
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_SIZE, 100L);
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_ALLOW_LATENCY, Duration.ofSeconds(1));
                });

        Rule rule =
                rule(
                        new WindowRule(WindowType.TUMBLE, Trigger.SINGLE, MINUTE, 0L, 0L),
                        new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null));

        testHarness.processElement1(rule, 0);
        testHarness.processBothWatermarks(new Watermark(-1L));
        testHarness.setProcessingTime(0L);

        testHarness.processElement2(row(1, "key1", "val1"), 0L);
        assertThat(metrics()).isEmpty();

        // the processing time timer of the buffer fires
        testHarness.setProcessingTime(1_000L);

        assertThat(metrics()).containsExactly(Tuple2.of(TimeUtil.longToString(0L), 1.0));
        assertThat(testHarness.numKeyedStateEntries()).isPositive();
    }

    /** The buffer is not a state, a checkpoint must not lose the elements it holds. */
    @Test
    void testMiniBatchFlushedBeforeCheckpoint() throws Exception {
        openHarness(
                conf -> {
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_ENABLED, true);
                    conf.set(RuleEngineOptions.AGG_MINI_BATCH_SIZE, 100L);
                    conf.set(
                            RuleEngineOptions.AGG_MINI_BATCH_ALLOW_LATENCY, Duration.ofSeconds(10));
                });

        Rule rule =
                rule(
                        new WindowRule(WindowType.TUMBLE, Trigger.BATCH, MINUTE, 0L, 0L),
                        new Aggregate("val_cnt", new String[] {"val"}, Method.COUNT, null));

        testHarness.processElement1(rule, 0);
        testHarness.processBothWatermarks(new Watermark(-1L));
        testHarness.processElement2(row(1, "key1", "val1"), 0L);

        assertThat(testHarness.numKeyedStateEntries()).isZero();

        testHarness.prepareSnapshotPreBarrier(1L);

        // the buffered element is part of the checkpoint
        assertThat(testHarness.numKeyedStateEntries()).isPositive();

        testHarness.processBothWatermarks(new Watermark(MINUTE));
        assertThat(metrics()).containsExactly(Tuple2.of(TimeUtil.longToString(MINUTE), 1.0));
    }

    // --------------------------------------------------------------------

    private void openHarness() throws Exception {
        openHarness(conf -> {});
    }

    private void openHarness(java.util.function.Consumer<Configuration> customize)
            throws Exception {
        testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new GlobalCoWindowAggOperator(),
                        null,
                        (Row value) -> Tuple2.of(value.getFieldAs(0), value.getFieldAs(1)),
                        Types.TUPLE(Types.INT, Types.STRING));

        Configuration conf = new Configuration();
        conf.set(RuleEngineOptions.JOB_ID, "test");
        conf.set(RuleEngineOptions.AGG_WINDOW_ALLOW_LATENESS, Duration.ZERO);
        customize.accept(conf);
        testHarness.getExecutionConfig().setGlobalJobParameters(conf);

        testHarness.setup();
        testHarness.open();
    }

    private static Rule rule(WindowRule window, Aggregate aggregate) {
        Rule rule = new Rule();
        rule.setJobId("test");
        rule.setRuleId(1);
        rule.setKeys(new String[] {"key"});
        rule.setRuleState(RuleState.ACTIVE);
        rule.setWindow(window);
        rule.setAggregates(new Aggregate[] {aggregate});
        rule.setThreshold(new Eval(aggregate.getName() + ">0", new String[] {aggregate.getName()}));
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

    /** The alert time and the metric of every alert, in output order. */
    private List<Tuple2<String, Double>> metrics() {
        return testHarness.getOutput().stream()
                .filter(StreamRecord.class::isInstance)
                .map(record -> (Row) ((StreamRecord<?>) record).getValue())
                .map(
                        row ->
                                Tuple2.of(
                                        row.<String>getFieldAs(1),
                                        row.<Map<String, Double>>getFieldAs(3).get("val_cnt")))
                .collect(Collectors.toList());
    }

    private List<Row> errors() {
        return testHarness.getSideOutput(ERROR_TAG).stream()
                .map(record -> (Row) ((StreamRecord<?>) record).getValue())
                .collect(Collectors.toList());
    }
}
