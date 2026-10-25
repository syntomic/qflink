package cn.syntomic.qflink.rule.entity;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class Rule {

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("rule_id")
    private int ruleId;

    @JsonProperty("rule_state")
    private RuleState ruleState;

    @JsonProperty("clear_state")
    private boolean clearState;

    private Eval filter;

    @JsonProperty("flat_map")
    private FlatMap flatMap;

    private WindowRule window;

    private String[] keys;

    private Aggregate[] aggregates;
    private Eval threshold;

    /** generic type */
    private Map<String, String> extras;

    public enum RuleState {
        ACTIVE,
        UPDATE,
        REMOVE
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public int getRuleId() {
        return ruleId;
    }

    public void setRuleId(int ruleId) {
        this.ruleId = ruleId;
    }

    public RuleState getRuleState() {
        return ruleState;
    }

    public void setRuleState(RuleState ruleState) {
        this.ruleState = ruleState;
    }

    public boolean isClearState() {
        return clearState;
    }

    public void setClearState(boolean clearState) {
        this.clearState = clearState;
    }

    public Eval getFilter() {
        return filter;
    }

    public void setFilter(Eval filter) {
        this.filter = filter;
    }

    public FlatMap getFlatMap() {
        return flatMap;
    }

    public void setFlatMap(FlatMap flatMap) {
        this.flatMap = flatMap;
    }

    public WindowRule getWindow() {
        return window;
    }

    public void setWindow(WindowRule window) {
        this.window = window;
    }

    public String[] getKeys() {
        return keys;
    }

    public void setKeys(String[] keys) {
        this.keys = keys;
    }

    public Aggregate[] getAggregates() {
        return aggregates;
    }

    public void setAggregates(Aggregate[] aggregates) {
        this.aggregates = aggregates;
    }

    public Eval getThreshold() {
        return threshold;
    }

    public void setThreshold(Eval threshold) {
        this.threshold = threshold;
    }

    public Map<String, String> getExtras() {
        return extras;
    }

    public void setExtras(Map<String, String> extras) {
        this.extras = extras;
    }
}
