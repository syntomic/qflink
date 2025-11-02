package cn.syntomic.qflink.rule.operators;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ERROR_TAG;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.KEY_SEPARATOR;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RULE_STATE_DESCRIPTOR;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_WINDOW_ALLOW_LATENESS;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.JOB_ID;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.exception.ExceptionUtils;

import cn.syntomic.qflink.common.utils.TimeUtil;
import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.entity.WindowRule.Trigger;
import cn.syntomic.qflink.rule.functions.aggregation.DynamicAggregateFunction;
import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;
import cn.syntomic.qflink.rule.functions.windowing.DynamicWindowAssigner;
import cn.syntomic.qflink.rule.utils.AviatorUtil;
import cn.syntomic.qflink.rule.utils.RowUtil;
import cn.syntomic.qflink.rule.utils.RuleUtil;

public class GlobalCoWindowAggOperator extends AbstractStreamOperator<Row>
        implements TwoInputStreamOperator<Rule, Row, Row>,
                Triggerable<Tuple2<Integer, String>, TimeWindow> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final DynamicWindowAssigner windowAssigner = new DynamicWindowAssigner();

    private transient TimestampedCollector<Row> collector;
    private transient BroadcastState<Integer, Rule> broadcastState;

    private AggregatingStateDescriptor<Row, Map<String, QAccumulator>, Map<String, Double>>
            windowStateDescriptor;
    private transient InternalAppendingState<
                    Tuple2<Integer, String>,
                    TimeWindow,
                    Row,
                    Map<String, QAccumulator>,
                    Map<String, Double>>
            windowState;

    protected transient InternalTimerService<TimeWindow> internalTimerService;

    private long allowedLateness;
    private String jobId;

    @Override
    @SuppressWarnings("unchecked")
    public void open() throws Exception {
        super.open();

        internalTimerService =
                getInternalTimerService("dynamic-window-timers", new TimeWindow.Serializer(), this);

        collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        Configuration conf = (Configuration) getExecutionConfig().getGlobalJobParameters();

        this.broadcastState = getOperatorStateBackend().getBroadcastState(RULE_STATE_DESCRIPTOR);
        this.windowStateDescriptor =
                new AggregatingStateDescriptor<>(
                        "dynamic-window-state",
                        new DynamicAggregateFunction(),
                        getRuntimeContext()
                                .createSerializer(
                                        Types.MAP(
                                                Types.STRING, Types.GENERIC(QAccumulator.class))));

        this.windowState =
                (InternalAppendingState<
                                Tuple2<Integer, String>,
                                TimeWindow,
                                Row,
                                Map<String, QAccumulator>,
                                Map<String, Double>>)
                        getOrCreateKeyedState(new TimeWindow.Serializer(), windowStateDescriptor);

        jobId = conf.get(JOB_ID);
        allowedLateness = conf.get(AGG_WINDOW_ALLOW_LATENESS).toMillis();
    }

    @Override
    public void processElement1(StreamRecord<Rule> element) throws Exception {
        try {
            if (jobId.equals(element.getValue().getJobId())) {
                RuleUtil.updateRule(element.getValue(), broadcastState);
                if (element.getValue().isClearState()) {
                    clearAllState(element.getValue().getRuleId());
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
        Rule rule =
                broadcastState.get(
                        this.<Tuple2<Integer, String>>getKeyedStateBackend().getCurrentKey().f0);

        if (rule != null) {
            try {
                // Same as window operator
                final Collection<TimeWindow> elementWindows =
                        windowAssigner.assignWindows(element.getTimestamp(), rule.getWindow());

                boolean isSkippedElement = true;

                for (TimeWindow window : elementWindows) {
                    if (isWindowLate(window)) {
                        continue;
                    }

                    isSkippedElement = false;

                    // Update window state
                    windowState.setCurrentNamespace(window);
                    windowState.add(Row.of(element.getValue(), rule.getAggregates()));

                    // trigger
                    TriggerResult triggerResult = onElement(window, rule.getWindow().getTrigger());
                    if (triggerResult.isFire() || triggerResult.isPurge()) {
                        Map<String, Double> contents = windowState.get();
                        if (contents == null) {
                            continue;
                        }
                        emitWindowContents(window, contents, rule, element);
                    }

                    // clean window state timer
                    internalTimerService.registerEventTimeTimer(window, cleanupTime(window));
                }

                // ! late element
                if (isSkippedElement && isElementLate(element.getTimestamp())) {
                    sideOutput(element.getValue(), rule, "Late Data", element.getTimestamp());
                }
            } catch (Exception e) {
                sideOutput(
                        element.getValue(),
                        rule,
                        ExceptionUtils.getStackTrace(e),
                        element.getTimestamp());
            }
        }
    }

    @Override
    public void onEventTime(InternalTimer<Tuple2<Integer, String>, TimeWindow> timer)
            throws Exception {
        Rule rule =
                broadcastState.get(
                        this.<Tuple2<Integer, String>>getKeyedStateBackend().getCurrentKey().f0);

        if (rule != null) {
            try {
                TimeWindow window = timer.getNamespace();
                windowState.setCurrentNamespace(window);

                TriggerResult triggerResult = onTriggerEventTime(timer.getTimestamp(), window);

                if (triggerResult.isPurge()) {
                    Map<String, Double> contents = windowState.get();
                    if (contents != null) {
                        emitWindowContents(window, contents, rule, null);
                    }
                }

                if (timer.getTimestamp() == cleanupTime(window)) {
                    // clean current namespace state
                    windowState.clear();
                }
            } catch (Exception e) {
                sideOutput(
                        Row.of("Timer fire error"),
                        rule,
                        ExceptionUtils.getStackTrace(e),
                        timer.getTimestamp());
            }
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<Tuple2<Integer, String>, TimeWindow> timer)
            throws Exception {
        // TODO No data trigger
        throw new UnsupportedOperationException("Unimplemented method 'onProcessingTime'");
    }

    // --------------------------------------------------------------------

    /** Clear all state of the rule id */
    private void clearAllState(int ruleId) throws Exception {
        KeyedStateBackend<Tuple2<Integer, String>> stateBackend = this.getKeyedStateBackend();

        try (Stream<Tuple2<Tuple2<Integer, String>, TimeWindow>> keyStream =
                this.<Tuple2<Integer, String>>getKeyedStateBackend()
                        .<TimeWindow>getKeysAndNamespaces(windowStateDescriptor.getName())) {

            // we copy the keys into list to avoid the concurrency problem
            // when state.clear() is invoked in function.process().
            final List<Tuple2<Tuple2<Integer, String>, TimeWindow>> keyAndNamespaces =
                    keyStream.collect(Collectors.toList());

            for (Tuple2<Tuple2<Integer, String>, TimeWindow> keyAndNamespace : keyAndNamespaces) {
                final State state =
                        stateBackend.getPartitionedState(
                                keyAndNamespace.f1,
                                new TimeWindow.Serializer(),
                                windowStateDescriptor);
                stateBackend.setCurrentKey(keyAndNamespace.f0);
                if (keyAndNamespace.f0.f0.equals(ruleId)) {
                    state.clear();
                }
            }
        }
    }

    /** Collector the window result */
    private void emitWindowContents(
            TimeWindow window, Map<String, Double> contents, Rule rule, StreamRecord<Row> element)
            throws Exception {

        if (AviatorUtil.alert(contents, rule.getThreshold())) {
            long alertTime = element == null ? window.maxTimestamp() : element.getTimestamp();
            Row alert =
                    Row.of(
                            rule.getRuleId(),
                            TimeUtil.longToString(
                                    element == null ? window.getEnd() : element.getTimestamp()),
                            getDims(
                                    this.<Tuple2<Integer, String>>getKeyedStateBackend()
                                            .getCurrentKey()
                                            .f1,
                                    rule.getKeys()),
                            contents,
                            element == null
                                    ? Collections.singletonMap("SOURCE", "timer")
                                    : RowUtil.toStringMap(element.getValue()));

            collector.setAbsoluteTimestamp(alertTime);
            collector.collect(alert);
        }
    }

    private Map<String, String> getDims(String values, String[] keys) throws Exception {
        // ? rule active time may not the same on different subtask
        // ? so length may not the same
        String[] valueArray = values.split(KEY_SEPARATOR);
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keys.length; i += 1) {
            map.put(keys[i], valueArray[i]);
        }

        return map;
    }

    private long cleanupTime(TimeWindow window) {
        return window.maxTimestamp() + allowedLateness;
    }

    private boolean isElementLate(long timestamp) {
        return timestamp + allowedLateness <= internalTimerService.currentWatermark();
    }

    private void sideOutput(Row row, Rule rule, String message, long timestamp) throws Exception {
        output.collect(
                ERROR_TAG,
                new StreamRecord<>(
                        Row.of(jobId, row.toString(), mapper.writeValueAsString(rule), message),
                        timestamp));
    }

    private TriggerResult onElement(TimeWindow window, Trigger trigger) {
        switch (trigger) {
            case SINGLE:
                return TriggerResult.FIRE;
            case BATCH:
                if (window.maxTimestamp() <= internalTimerService.currentWatermark()) {
                    return TriggerResult.PURGE;
                } else {
                    internalTimerService.registerEventTimeTimer(window, window.maxTimestamp());
                }
                return TriggerResult.CONTINUE;
            default:
                throw new UnsupportedOperationException(trigger + " trigger not implemented yet.");
        }
    }

    private boolean isWindowLate(TimeWindow window) {
        return cleanupTime(window) <= internalTimerService.currentWatermark();
    }

    private TriggerResult onTriggerEventTime(long timestamp, TimeWindow window) {
        return timestamp == window.maxTimestamp() ? TriggerResult.PURGE : TriggerResult.CONTINUE;
    }
}
