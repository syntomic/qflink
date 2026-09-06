package cn.syntomic.qflink.rule.operators;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ERROR_TAG;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RULE_STATE_DESCRIPTOR;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_MINI_BATCH_ALLOW_LATENCY;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_MINI_BATCH_SIZE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.JOB_ID;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.exception.ExceptionUtils;

import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.entity.Rule.RuleState;
import cn.syntomic.qflink.rule.functions.aggregation.DynamicAggregateFunction;
import cn.syntomic.qflink.rule.functions.windowing.DynamicSliceAssigner;
import cn.syntomic.qflink.rule.functions.windowing.MiniBatchWindowBuffer;
import cn.syntomic.qflink.rule.functions.windowing.MiniBatchWindowBuffer.BufferedSlice;
import cn.syntomic.qflink.rule.utils.RuleUtil;

/**
 * Aggregate the slices of a rule <b>before the shuffle</b>, the first phase of the two phase
 * aggregation.
 *
 * <p>The elements are aggregated in memory by key and slice, and only the accumulators cross the
 * network: {@code (rule_id, key, slice_end, accumulators, extras)}. The {@link
 * GlobalCoWindowAggOperator} merges them into the window state, which is the second phase.
 *
 * <p>The operator holds no keyed state, its buffer is flushed before a watermark, before a
 * checkpoint, when it is full and after the allowed latency, exactly like the mini batch of the
 * aggregate operator.
 */
public class LocalWindowAggOperator extends AbstractStreamOperator<Row>
        implements TwoInputStreamOperator<Rule, Row, Row> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final DynamicSliceAssigner sliceAssigner = new DynamicSliceAssigner();
    private final DynamicAggregateFunction aggFunction = new DynamicAggregateFunction();

    private transient TimestampedCollector<Row> collector;
    private transient BroadcastState<Integer, Rule> broadcastState;
    private transient MiniBatchWindowBuffer<Tuple2<Integer, String>> buffer;

    private transient boolean flushTimerRegistered;

    private long miniBatchSize;
    private long miniBatchLatency;
    private String jobId;

    @Override
    public void open() throws Exception {
        super.open();

        collector = new TimestampedCollector<>(output);
        this.broadcastState = getOperatorStateBackend().getBroadcastState(RULE_STATE_DESCRIPTOR);

        Configuration conf = Configuration.fromMap(getRuntimeContext().getGlobalJobParameters());
        jobId = conf.get(JOB_ID);
        miniBatchSize = conf.get(AGG_MINI_BATCH_SIZE);
        miniBatchLatency = conf.get(AGG_MINI_BATCH_ALLOW_LATENCY).toMillis();

        buffer = new MiniBatchWindowBuffer<>(aggFunction);
    }

    @Override
    public void processElement1(StreamRecord<Rule> element) throws Exception {
        try {
            Rule rule = element.getValue();
            if (jobId.equals(rule.getJobId())) {
                if (rule.getRuleState() != RuleState.REMOVE) {
                    sliceAssigner.validate(rule.getWindow());
                }

                RuleUtil.updateRule(rule, broadcastState);
                if (rule.isClearState()) {
                    // the buffered elements must not resurrect the state of the rule
                    buffer.removeIf(key -> key.f0 == rule.getRuleId());
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
        Row row = element.getValue();
        Rule rule = null;

        try {
            Tuple2<Integer, String> key = Tuple2.of(row.getFieldAs(0), row.getFieldAs(1));
            rule = broadcastState.get(key.f0);

            if (rule == null) {
                return;
            }

            // ! the elements of a slice are aggregated here, only the accumulators are shuffled
            buffer.add(
                    key,
                    sliceAssigner.assignSliceEnd(element.getTimestamp(), rule.getWindow()),
                    row,
                    element.getTimestamp(),
                    rule.getAggregates());

            registerFlushTimer();
            if (buffer.count() >= miniBatchSize) {
                flush();
            }
        } catch (Exception e) {
            collector.collect(
                    ERROR_TAG,
                    new StreamRecord<>(
                            Row.of(
                                    jobId,
                                    row.toString(),
                                    rule == null ? null : mapper.writeValueAsString(rule),
                                    ExceptionUtils.getStackTrace(e)),
                            element.getTimestamp()));
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // ! the partial results must be downstream before the watermark which fires their windows
        flush();
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // the buffer is not a state, it has to be downstream before the barrier
        flush();
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void finish() throws Exception {
        flush();
        super.finish();
    }

    // --------------------------------------------------------------------

    /** Emit one partial result per key and slice */
    private void flush() {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        for (Entry<Tuple2<Integer, String>, Map<Long, BufferedSlice>> keyEntry : buffer.entries()) {
            for (Entry<Long, BufferedSlice> sliceEntry : keyEntry.getValue().entrySet()) {
                BufferedSlice slice = sliceEntry.getValue();

                collector.setAbsoluteTimestamp(slice.getLastTimestamp());
                collector.collect(
                        Row.of(
                                keyEntry.getKey().f0,
                                keyEntry.getKey().f1,
                                sliceEntry.getKey(),
                                slice.getAccumulators(),
                                slice.getExtras()));
            }
        }

        buffer.clear();
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
}
