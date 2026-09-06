package cn.syntomic.qflink.rule.operators;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ERROR_TAG;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.KEY_SEPARATOR;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.PARTIAL_ACCUMULATORS;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.PARTIAL_EXTRAS;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.PARTIAL_SLICE_END;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RULE_STATE_DESCRIPTOR;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_LOCAL_ENABLED;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_MINI_BATCH_ALLOW_LATENCY;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_MINI_BATCH_ENABLED;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_MINI_BATCH_SIZE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_WINDOW_ALLOW_LATENESS;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.JOB_ID;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.exception.ExceptionUtils;

import cn.syntomic.qflink.common.utils.TimeUtil;
import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.entity.Rule.RuleState;
import cn.syntomic.qflink.rule.entity.WindowRule;
import cn.syntomic.qflink.rule.entity.WindowRule.Trigger;
import cn.syntomic.qflink.rule.functions.aggregation.DynamicAggregateFunction;
import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;
import cn.syntomic.qflink.rule.functions.windowing.DynamicSliceAssigner;
import cn.syntomic.qflink.rule.functions.windowing.MiniBatchWindowBuffer;
import cn.syntomic.qflink.rule.functions.windowing.MiniBatchWindowBuffer.BufferedSlice;
import cn.syntomic.qflink.rule.utils.AviatorUtil;
import cn.syntomic.qflink.rule.utils.RowUtil;
import cn.syntomic.qflink.rule.utils.RuleUtil;

/**
 * Aggregate the keyed stream by the window of the rule, sliced like the window table valued
 * functions of Flink SQL.
 *
 * <p>An element is accumulated into <b>one</b> slice only, a window result is computed by merging
 * the slices it is composed of. A cumulate window merges the slice which just ended into the first
 * slice of the window, so its state does not grow with the number of steps.
 *
 * <p>With {@code agg.mini-batch.enabled} the elements are aggregated in memory first and the buffer
 * is flushed as a whole, so a slice is read and written once per batch instead of once per element.
 */
public class GlobalCoWindowAggOperator extends AbstractStreamOperator<Row>
        implements TwoInputStreamOperator<Rule, Row, Row>,
                Triggerable<Tuple2<Integer, String>, TimeWindow> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final DynamicSliceAssigner sliceAssigner = new DynamicSliceAssigner();
    private final DynamicAggregateFunction aggFunction = new DynamicAggregateFunction();

    private transient TimestampedCollector<Row> collector;
    private transient BroadcastState<Integer, Rule> broadcastState;

    private ValueStateDescriptor<Map<String, QAccumulator>> sliceStateDescriptor;

    /** The accumulators of a slice, the namespace is the slice. */
    private transient InternalValueState<
                    Tuple2<Integer, String>, TimeWindow, Map<String, QAccumulator>>
            sliceState;

    protected transient InternalTimerService<TimeWindow> internalTimerService;

    /** The elements which are not in the state yet, null when the mini batch is disabled. */
    private transient MiniBatchWindowBuffer<Tuple2<Integer, String>> buffer;

    private transient boolean flushTimerRegistered;

    /** The input is the partial result of a local aggregate, not the elements themselves. */
    private boolean localAggEnabled;

    private long allowedLateness;
    private long miniBatchSize;
    private long miniBatchLatency;
    private String jobId;

    @Override
    @SuppressWarnings("unchecked")
    public void open() throws Exception {
        super.open();

        internalTimerService =
                getInternalTimerService("dynamic-window-timers", new TimeWindow.Serializer(), this);

        collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        Configuration conf = Configuration.fromMap(getRuntimeContext().getGlobalJobParameters());

        this.broadcastState = getOperatorStateBackend().getBroadcastState(RULE_STATE_DESCRIPTOR);
        this.sliceStateDescriptor =
                new ValueStateDescriptor<>(
                        "dynamic-window-state",
                        getRuntimeContext()
                                .createSerializer(
                                        Types.MAP(
                                                Types.STRING, Types.GENERIC(QAccumulator.class))));

        this.sliceState =
                (InternalValueState<Tuple2<Integer, String>, TimeWindow, Map<String, QAccumulator>>)
                        getOrCreateKeyedState(new TimeWindow.Serializer(), sliceStateDescriptor);

        jobId = conf.get(JOB_ID);
        allowedLateness = conf.get(AGG_WINDOW_ALLOW_LATENESS).toMillis();
        localAggEnabled = conf.get(AGG_LOCAL_ENABLED);

        if (conf.get(AGG_MINI_BATCH_ENABLED)) {
            miniBatchSize = conf.get(AGG_MINI_BATCH_SIZE);
            miniBatchLatency = conf.get(AGG_MINI_BATCH_ALLOW_LATENCY).toMillis();
            buffer = new MiniBatchWindowBuffer<>(aggFunction);
        }
    }

    @Override
    public void processElement1(StreamRecord<Rule> element) throws Exception {
        try {
            Rule rule = element.getValue();
            if (jobId.equals(rule.getJobId())) {
                if (rule.getRuleState() != RuleState.REMOVE) {
                    // a window which cannot be sliced is reported once, not on every element
                    sliceAssigner.validate(rule.getWindow());
                }

                RuleUtil.updateRule(rule, broadcastState);
                if (rule.isClearState()) {
                    // the buffered elements would resurrect the state right after it was cleared
                    flush();
                    clearAllState(rule.getRuleId());
                }
            }
        } catch (Exception e) {
            collector.collect(
                    ERROR_TAG,
                    new StreamRecord<>(
                            Row.of(
                                    jobId,
                                    "Handle rule error",
                                    mapper.writeValueAsString(element.getValue()),
                                    ExceptionUtils.getStackTrace(e))));
        }
    }

    @Override
    public void processElement2(StreamRecord<Row> element) throws Exception {
        Tuple2<Integer, String> key =
                this.<Tuple2<Integer, String>>getKeyedStateBackend().getCurrentKey();
        Rule rule = broadcastState.get(key.f0);

        if (rule == null) {
            return;
        }

        try {
            WindowRule windowRule = rule.getWindow();
            Row value = element.getValue();

            // the slice of a partial result has been assigned before the shuffle
            long sliceEnd =
                    localAggEnabled
                            ? value.<Long>getFieldAs(PARTIAL_SLICE_END)
                            : sliceAssigner.assignSliceEnd(element.getTimestamp(), windowRule);

            // ! late element
            if (isSliceLate(sliceEnd, windowRule)) {
                sideOutput(value, rule, "Late Data", element.getTimestamp());
                return;
            }

            if (buffer != null) {
                // ! aggregate in memory, the state is only touched when the buffer is flushed
                if (localAggEnabled) {
                    buffer.merge(
                            key,
                            sliceEnd,
                            value.getFieldAs(PARTIAL_ACCUMULATORS),
                            value.getFieldAs(PARTIAL_EXTRAS),
                            element.getTimestamp());
                } else {
                    buffer.add(key, sliceEnd, value, element.getTimestamp(), rule.getAggregates());
                }

                registerFlushTimer();
                if (buffer.count() >= miniBatchSize) {
                    flush();
                }
                return;
            }

            TimeWindow slice = sliceAssigner.sliceOf(sliceEnd, windowRule);
            if (localAggEnabled) {
                // ! the second phase only merges what the first phase already aggregated
                mergeIntoState(slice, value.getFieldAs(PARTIAL_ACCUMULATORS));
                fireOnElement(
                        sliceEnd, rule, value.getFieldAs(PARTIAL_EXTRAS), element.getTimestamp());
            } else {
                // ! the element is accumulated once, whatever the number of windows it belongs to
                accumulate(slice, value, rule);
                fireOnElement(sliceEnd, rule, RowUtil.toStringMap(value), element.getTimestamp());
            }
        } catch (Exception e) {
            sideOutput(
                    element.getValue(),
                    rule,
                    ExceptionUtils.getStackTrace(e),
                    element.getTimestamp());
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // ! the buffered elements must be in the state before the windows of the watermark fire
        flush();
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // the buffer is not a state, it has to be in the state before the barrier
        flush();
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void finish() throws Exception {
        flush();
        super.finish();
    }

    @Override
    public void onEventTime(InternalTimer<Tuple2<Integer, String>, TimeWindow> timer)
            throws Exception {
        Rule rule =
                broadcastState.get(
                        this.<Tuple2<Integer, String>>getKeyedStateBackend().getCurrentKey().f0);

        if (rule == null) {
            return;
        }

        try {
            WindowRule windowRule = rule.getWindow();
            long end = timer.getNamespace().getEnd();

            // ! a window timer fires at the last timestamp of the window it is registered for,
            // ! a cleanup timer one millisecond after the last window of its slice
            if (timer.getTimestamp() == end - 1) {
                fireWindowTimer(end, rule);
            }

            if (timer.getTimestamp() == cleanupTime(end, windowRule)) {
                // the slice is not needed by any window anymore
                sliceState.setCurrentNamespace(sliceAssigner.sliceOf(end, windowRule));
                sliceState.clear();
            }
        } catch (Exception e) {
            sideOutput(
                    Row.of("Timer fire error"),
                    rule,
                    ExceptionUtils.getStackTrace(e),
                    timer.getTimestamp());
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<Tuple2<Integer, String>, TimeWindow> timer)
            throws Exception {
        // TODO No data trigger
        throw new UnsupportedOperationException("Unimplemented method 'onProcessingTime'");
    }

    // --------------------------------------------------------------------

    /** Write the buffered elements to the state and fire what the elements trigger */
    private void flush() throws Exception {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        KeyedStateBackend<Tuple2<Integer, String>> stateBackend = getKeyedStateBackend();
        Tuple2<Integer, String> currentKey = stateBackend.getCurrentKey();

        for (Entry<Tuple2<Integer, String>, Map<Long, BufferedSlice>> keyEntry : buffer.entries()) {
            stateBackend.setCurrentKey(keyEntry.getKey());
            Rule rule = broadcastState.get(keyEntry.getKey().f0);

            if (rule == null) {
                // the rule has been removed while its elements were buffered
                continue;
            }

            for (Entry<Long, BufferedSlice> sliceEntry : keyEntry.getValue().entrySet()) {
                BufferedSlice slice = sliceEntry.getValue();
                try {
                    mergeIntoState(
                            sliceAssigner.sliceOf(sliceEntry.getKey(), rule.getWindow()),
                            slice.getAccumulators());
                    fireOnElement(
                            sliceEntry.getKey(), rule, slice.getExtras(), slice.getLastTimestamp());
                } catch (Exception e) {
                    sideOutput(
                            slice.getLastRow(),
                            rule,
                            ExceptionUtils.getStackTrace(e),
                            slice.getLastTimestamp());
                }
            }
        }

        buffer.clear();
        if (currentKey != null) {
            stateBackend.setCurrentKey(currentKey);
        }
    }

    /** The buffer is flushed at the latest after the allowed latency */
    private void registerFlushTimer() {
        if (flushTimerRegistered) {
            return;
        }

        flushTimerRegistered = true;
        getProcessingTimeService()
                .registerTimer(
                        getProcessingTimeService().getCurrentProcessingTime() + miniBatchLatency,
                        timestamp -> {
                            flushTimerRegistered = false;
                            flush();
                        });
    }

    /** Register the timers of the slice and fire what an element triggers */
    private void fireOnElement(long sliceEnd, Rule rule, Map<String, String> extras, long timestamp)
            throws Exception {
        WindowRule windowRule = rule.getWindow();

        // the window ending with this slice fires when the watermark passes it
        registerWindowTimer(sliceEnd, windowRule);
        registerCleanupTimer(sliceEnd, windowRule);

        switch (windowRule.getTrigger()) {
            case SINGLE:
                // early fire the window which ends with this slice
                emitWindowContents(
                        sliceEnd, mergeSlices(sliceEnd, windowRule), rule, extras, timestamp);
                break;
            case BATCH:
                if (sliceEnd - 1 <= internalTimerService.currentWatermark()) {
                    // the window already ended, its timer will not fire again
                    emitWindowContents(
                            sliceEnd, fireWindow(sliceEnd, windowRule), rule, extras, timestamp);
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        windowRule.getTrigger() + " trigger not implemented yet.");
        }
    }

    /** Fire the window which just ended and trigger the next window sharing its slices */
    private void fireWindowTimer(long windowEnd, Rule rule) throws Exception {
        WindowRule windowRule = rule.getWindow();
        Map<String, QAccumulator> merged = fireWindow(windowEnd, windowRule);

        if (merged == null || merged.isEmpty()) {
            // nothing left in the slices, the following windows are empty as well
            return;
        }

        if (windowRule.getTrigger() == Trigger.BATCH) {
            emitWindowContents(
                    windowEnd,
                    merged,
                    rule,
                    Collections.singletonMap("SOURCE", "timer"),
                    windowEnd);
        }

        // ! the following windows share the slices of this one, they have no timer of their own
        OptionalLong next = sliceAssigner.nextWindowEnd(windowEnd, windowRule);
        if (next.isPresent()) {
            registerWindowTimer(next.getAsLong(), windowRule);
        }
    }

    /**
     * Compute the window which ends at the given time
     *
     * <p>A cumulate window merges the slice which just ended into the first slice of the window, so
     * the next step only has to merge one more slice.
     *
     * @param windowEnd
     * @param windowRule
     * @return the merged accumulators, null when every slice is empty
     */
    private Map<String, QAccumulator> fireWindow(long windowEnd, WindowRule windowRule)
            throws IOException {
        OptionalLong target = sliceAssigner.mergeTarget(windowEnd, windowRule);

        if (target.isPresent() && target.getAsLong() != windowEnd) {
            // the target slice may have been created by the merge only
            registerCleanupTimer(target.getAsLong(), windowRule);

            return mergeIntoTarget(
                    sliceAssigner.sliceOf(target.getAsLong(), windowRule),
                    sliceAssigner.sliceOf(windowEnd, windowRule));
        }

        return mergeSlices(windowEnd, windowRule);
    }

    /**
     * Merge the slices of the window which ends at the given time
     *
     * @param windowEnd
     * @param windowRule
     * @return the merged accumulators, null when every slice is empty
     */
    private Map<String, QAccumulator> mergeSlices(long windowEnd, WindowRule windowRule)
            throws IOException {
        Map<String, QAccumulator> merged = null;

        for (long sliceEnd : sliceAssigner.slicesOf(windowEnd, windowRule)) {
            sliceState.setCurrentNamespace(sliceAssigner.sliceOf(sliceEnd, windowRule));
            Map<String, QAccumulator> slice = sliceState.value();

            if (slice == null || slice.isEmpty()) {
                continue;
            }

            // ! the accumulators of a slice stay in the state, they must not be modified
            merged =
                    merged == null
                            ? DynamicAggregateFunction.copy(slice)
                            : aggFunction.merge(merged, slice);
        }

        return merged;
    }

    /** Accumulate one element into its slice */
    private void accumulate(TimeWindow slice, Row row, Rule rule) throws IOException {
        sliceState.setCurrentNamespace(slice);

        Map<String, QAccumulator> accumulator = sliceState.value();
        if (accumulator == null) {
            accumulator = aggFunction.createAccumulator();
        }

        sliceState.update(aggFunction.add(Row.of(row, rule.getAggregates()), accumulator));
    }

    /** Merge the accumulators of a mini batch into the state of the slice */
    private void mergeIntoState(TimeWindow slice, Map<String, QAccumulator> buffered)
            throws IOException {
        sliceState.setCurrentNamespace(slice);

        Map<String, QAccumulator> accumulator = sliceState.value();
        sliceState.update(
                accumulator == null ? buffered : aggFunction.merge(accumulator, buffered));
    }

    /** Merge an ended slice into the slice the window accumulates in, and drop it */
    private Map<String, QAccumulator> mergeIntoTarget(TimeWindow target, TimeWindow source)
            throws IOException {
        sliceState.setCurrentNamespace(source);
        Map<String, QAccumulator> sourceAcc = sliceState.value();

        sliceState.setCurrentNamespace(target);
        Map<String, QAccumulator> targetAcc = sliceState.value();

        if (sourceAcc == null || sourceAcc.isEmpty()) {
            return targetAcc;
        }

        targetAcc = targetAcc == null ? sourceAcc : aggFunction.merge(targetAcc, sourceAcc);
        sliceState.update(targetAcc);

        sliceState.setCurrentNamespace(source);
        sliceState.clear();

        return targetAcc;
    }

    /** Clear all state of the rule id */
    private void clearAllState(int ruleId) throws Exception {
        KeyedStateBackend<Tuple2<Integer, String>> stateBackend = this.getKeyedStateBackend();

        try (Stream<Tuple2<Tuple2<Integer, String>, TimeWindow>> keyStream =
                this.<Tuple2<Integer, String>>getKeyedStateBackend()
                        .<TimeWindow>getKeysAndNamespaces(sliceStateDescriptor.getName())) {

            // we copy the keys into list to avoid the concurrency problem
            // when state.clear() is invoked in function.process().
            final List<Tuple2<Tuple2<Integer, String>, TimeWindow>> keyAndNamespaces =
                    keyStream.collect(Collectors.toList());

            for (Tuple2<Tuple2<Integer, String>, TimeWindow> keyAndNamespace : keyAndNamespaces) {
                final State state =
                        stateBackend.getPartitionedState(
                                keyAndNamespace.f1,
                                new TimeWindow.Serializer(),
                                sliceStateDescriptor);
                stateBackend.setCurrentKey(keyAndNamespace.f0);
                if (keyAndNamespace.f0.f0.equals(ruleId)) {
                    state.clear();
                }
            }
        }
    }

    /**
     * Collector the window result
     *
     * @param windowEnd
     * @param merged the accumulators of the window
     * @param rule
     * @param extras the context of the alert: the last element, or the source when a timer fired
     * @param timestamp
     */
    private void emitWindowContents(
            long windowEnd,
            Map<String, QAccumulator> merged,
            Rule rule,
            Map<String, String> extras,
            long timestamp)
            throws Exception {

        if (merged == null || merged.isEmpty()) {
            return;
        }

        Map<String, Double> contents = DynamicAggregateFunction.toResult(merged);

        if (AviatorUtil.alert(contents, rule.getThreshold())) {
            Row alert =
                    Row.of(
                            rule.getRuleId(),
                            TimeUtil.longToString(timestamp),
                            getDims(
                                    this.<Tuple2<Integer, String>>getKeyedStateBackend()
                                            .getCurrentKey()
                                            .f1,
                                    rule.getKeys()),
                            contents,
                            extras);

            // the alert never carries a timestamp after the window it belongs to
            collector.setAbsoluteTimestamp(Math.min(timestamp, windowEnd - 1));
            collector.collect(alert);
        }
    }

    private Map<String, String> getDims(String values, String[] keys) {
        // ! rule active time may not be the same on different subtasks,
        // ! so the number of values may not match the number of keys
        String[] valueArray = values.split(KEY_SEPARATOR, -1);
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keys.length; i += 1) {
            map.put(keys[i], i < valueArray.length ? valueArray[i] : null);
        }

        return map;
    }

    /** The window which ends with the slice fires when the watermark passes its last timestamp */
    private void registerWindowTimer(long windowEnd, WindowRule windowRule) {
        internalTimerService.registerEventTimeTimer(
                sliceAssigner.windowOf(windowEnd, windowRule), windowEnd - 1);
    }

    /** The slice is dropped once its last window has been fired */
    private void registerCleanupTimer(long sliceEnd, WindowRule windowRule) {
        internalTimerService.registerEventTimeTimer(
                sliceAssigner.sliceOf(sliceEnd, windowRule), cleanupTime(sliceEnd, windowRule));
    }

    /**
     * The slice is dropped one millisecond after its last window fired, so that the cleanup never
     * races with the window it belongs to.
     */
    private long cleanupTime(long sliceEnd, WindowRule windowRule) {
        return sliceAssigner.lastWindowEnd(sliceEnd, windowRule) + allowedLateness;
    }

    /** Every window the slice belongs to has been fired and the lateness has elapsed */
    private boolean isSliceLate(long sliceEnd, WindowRule windowRule) {
        return cleanupTime(sliceEnd, windowRule) <= internalTimerService.currentWatermark();
    }

    private void sideOutput(Row row, Rule rule, String message, long timestamp) throws Exception {
        output.collect(
                ERROR_TAG,
                new StreamRecord<>(
                        Row.of(jobId, row.toString(), mapper.writeValueAsString(rule), message),
                        timestamp));
    }
}
