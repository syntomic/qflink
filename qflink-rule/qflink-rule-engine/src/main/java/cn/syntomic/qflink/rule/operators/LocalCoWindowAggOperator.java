package cn.syntomic.qflink.rule.operators;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ERROR_TAG;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ETL_TABLE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.KEY;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.KEY_SEPARATOR;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RAW_FIELD;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RULE_ID;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RULE_STATE_DESCRIPTOR;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_ENABLE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.ETL_DEFAULT_TABLE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.JOB_ID;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import cn.syntomic.qflink.common.utils.JsonUtil;
import cn.syntomic.qflink.rule.entity.FlatMap;
import cn.syntomic.qflink.rule.entity.NormalField;
import cn.syntomic.qflink.rule.entity.NormalField.NormalFieldType;
import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.utils.AviatorUtil;
import cn.syntomic.qflink.rule.utils.RowUtil;
import cn.syntomic.qflink.rule.utils.RuleUtil;

public class LocalCoWindowAggOperator extends AbstractStreamOperator<Row>
        implements TwoInputStreamOperator<Rule, Row, Row> {

    private static final long serialVersionUID = 1L;

    private String jobId;
    private String defaultTable;
    private boolean aggEnable;

    private transient TimestampedCollector<Row> collector;
    private transient BroadcastState<Integer, Rule> broadcastState;

    /** Compiling a pattern per record is expensive, the rules change rarely. */
    private transient Map<String, Pattern> patternCache;

    @Override
    public void open() throws Exception {
        super.open();

        collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        this.patternCache = new HashMap<>();
        this.broadcastState = getOperatorStateBackend().getBroadcastState(RULE_STATE_DESCRIPTOR);

        Configuration conf = Configuration.fromMap(getRuntimeContext().getGlobalJobParameters());
        jobId = conf.get(JOB_ID);
        defaultTable = conf.get(ETL_DEFAULT_TABLE);
        aggEnable = conf.get(AGG_ENABLE);

        JsonUtil.openJsonPath(null, 0);
    }

    @Override
    public void processElement1(StreamRecord<Rule> element) throws Exception {
        try {
            if (jobId.equals(element.getValue().getJobId())) {
                RuleUtil.updateRule(element.getValue(), broadcastState);
            }
        } catch (Exception e) {
            collector.collect(
                    ERROR_TAG,
                    new StreamRecord<>(
                            Row.of(
                                    jobId,
                                    "Rule Engine: ETL handle rule error",
                                    JsonUtil.getCommonMapper()
                                            .writeValueAsString(element.getValue()),
                                    ExceptionUtils.getStackTrace(e))));
        }
    }

    @Override
    public void processElement2(StreamRecord<Row> element) throws Exception {
        try {
            for (Map.Entry<Integer, Rule> entry : broadcastState.immutableEntries()) {
                boolean isNeed =
                        AviatorUtil.filterData(element.getValue(), entry.getValue().getFilter());

                if (!isNeed) {
                    continue;
                }

                // ! every rule gets its own row, the input record must never be mutated
                Row row =
                        handleFlatMap(Row.copy(element.getValue()), entry.getValue().getFlatMap());

                if (row == null) {
                    // the log does not match the pattern of the rule
                    sideOutput(
                            element.getValue(),
                            entry.getValue(),
                            "Log does not match the pattern of the rule");
                    continue;
                }

                if (aggEnable) {
                    row = handleKey(row, entry.getValue());
                    collector.setAbsoluteTimestamp(element.getTimestamp());
                } else {
                    Map<String, String> extras = entry.getValue().getExtras();
                    row.setField(
                            ETL_TABLE,
                            extras == null
                                    ? defaultTable
                                    : extras.getOrDefault(ETL_TABLE, defaultTable));
                }

                collector.collect(row);
            }
        } catch (Exception e) {
            collector.collect(
                    ERROR_TAG,
                    new StreamRecord<>(
                            Row.of(
                                    jobId,
                                    "Handle rule error",
                                    JsonUtil.getCommonMapper()
                                            .writeValueAsString(element.getValue()),
                                    ExceptionUtils.getStackTrace(e))));
        }
    }

    /** Report a record which cannot be handled by the rule instead of dropping it silently */
    private void sideOutput(Row row, Rule rule, String message) throws Exception {
        collector.collect(
                ERROR_TAG,
                new StreamRecord<>(
                        Row.of(
                                jobId,
                                JsonUtil.getCommonMapper().writeValueAsString(row),
                                JsonUtil.getCommonMapper().writeValueAsString(rule),
                                message)));
    }

    /**
     * Flat the raw log to a named row, null when the log does not match the pattern of the rule
     *
     * @param row
     * @param flatMap
     * @return
     */
    private Row handleFlatMap(Row row, FlatMap flatMap) {

        if (flatMap == null) {
            return row;
        }

        if (flatMap.getPattern() != null) {
            Matcher m = compilePattern(flatMap.getPattern()).matcher(row.getFieldAs(RAW_FIELD));

            if (!m.matches()) {
                return null;
            }

            // reuse row
            row = Row.withNames();
            // normal field
            NormalField[] normalFields = flatMap.getNormalFields();
            for (int i = 0; i < normalFields.length; i++) {
                if (normalFields[i].getType() == NormalFieldType.JSON) {
                    JsonUtil.evalJsonPaths(
                            m.group(i + 1), normalFields[i].getJsonPaths(), row::setField);
                } else {
                    row.setField(
                            normalFields[i].getName(),
                            StringUtils.isBlank(m.group(i + 1))
                                    ? normalFields[i].getDefaulz()
                                    : m.group(i + 1));
                }
            }
        }

        // extend field
        if (flatMap.getMappingFields() != null && row.getArity() > 0) {
            row = AviatorUtil.evalMapping(row, flatMap.getMappingFields());
        }

        // TODO Local aggregate beacause we already have the rule of aggregate
        return row;
    }

    private Pattern compilePattern(String pattern) {
        return patternCache.computeIfAbsent(pattern, Pattern::compile);
    }

    private Row handleKey(Row row, Rule rule) {
        String logKey =
                String.join(
                        KEY_SEPARATOR,
                        Stream.of(rule.getKeys())
                                .map(key -> Objects.toString(row.getField(key), ""))
                                .toArray(String[]::new));

        return RowUtil.join(row, new String[] {RULE_ID, KEY}, rule.getRuleId(), logKey);
    }
}
