package cn.syntomic.qflink.web.rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.fasterxml.jackson.databind.JsonNode;
import com.googlecode.aviator.AviatorEvaluator;

/**
 * Checks a rule before it is published, so a broken one is refused by the console instead of ending
 * up in the error side output of every element.
 *
 * <p>The checks are the constraints of the engine itself: the window must be sliceable, an
 * aggregate always reads its first input, and the expressions are compiled by the very evaluator
 * the job uses.
 */
public class RuleValidator {

    private static final Set<String> RULE_STATES = Set.of("ACTIVE", "UPDATE", "REMOVE");
    private static final Set<String> WINDOW_TYPES = Set.of("TUMBLE", "CUMULATE", "SLIDE");
    private static final Set<String> TRIGGERS = Set.of("BATCH", "SINGLE", "CONTINUOUS", "HYBRID");
    private static final Set<String> METHODS =
            Set.of("COUNT_DISTINCT", "COUNT", "SUM", "MAX", "MIN", "AVG");
    private static final Set<String> NORMAL_FIELD_TYPES = Set.of("STRING", "JSON");
    private static final Set<String> FIELD_TYPES =
            Set.of("STRING", "BOOLEAN", "INT", "BIGINT", "FLOAT", "DOUBLE", "MAP", "LIST");

    private final String jobId;
    private final boolean aggEnabled;
    private final EtlSchema schema;

    public RuleValidator(String jobId, boolean aggEnabled, EtlSchema schema) {
        this.jobId = jobId;
        this.aggEnabled = aggEnabled;
        this.schema = schema;
    }

    /** What a validation found, errors refuse the rule and warnings only annotate it. */
    public static class Result {

        private final List<String> errors = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();

        public List<String> getErrors() {
            return errors;
        }

        public List<String> getWarnings() {
            return warnings;
        }

        public boolean isValid() {
            return errors.isEmpty();
        }

        private void error(String format, Object... args) {
            errors.add(String.format(format, args));
        }

        private void warn(String format, Object... args) {
            warnings.add(String.format(format, args));
        }
    }

    /**
     * Validate a rule document
     *
     * @param rule
     * @return
     */
    public Result validate(JsonNode rule) {
        Result result = new Result();
        if (rule == null || !rule.isObject()) {
            result.error("A rule must be a json object");
            return result;
        }

        validateIdentity(rule, result);
        validateEval(rule.path("filter"), "filter", result);
        validateFlatMap(rule.path("flat_map"), result);

        if (aggEnabled) {
            validateKeys(rule.path("keys"), result);
            validateWindow(rule.path("window"), result);
            Set<String> metrics = validateAggregates(rule.path("aggregates"), result);
            validateThreshold(rule.path("threshold"), metrics, result);
        }

        return result;
    }

    // -------------------------------------------------------------------------

    private void validateIdentity(JsonNode rule, Result result) {
        JsonNode ruleId = rule.path("rule_id");
        if (!ruleId.isInt() || ruleId.asInt() <= 0) {
            result.error("rule_id must be a positive integer, but got %s", ruleId.asText("null"));
        }

        String ruleJobId = rule.path("job_id").asText("");
        if (ruleJobId.isBlank()) {
            result.error("job_id is required, the engine only applies the rules of its own job");
        } else if (jobId != null && !jobId.equals(ruleJobId)) {
            result.error(
                    "job_id %s is not the job of this engine(%s), the aggregate would ignore it",
                    ruleJobId, jobId);
        }

        String state = rule.path("rule_state").asText("");
        if (!RULE_STATES.contains(state)) {
            result.error("rule_state must be one of %s, but got %s", RULE_STATES, state);
        }
    }

    private void validateEval(JsonNode eval, String name, Result result) {
        if (eval.isMissingNode() || eval.isNull()) {
            return;
        }

        String expr = eval.path("expr").asText("");
        if (expr.isBlank()) {
            result.error("%s.expr must not be blank, drop the %s instead", name, name);
            return;
        }

        try {
            AviatorEvaluator.compile(expr, true);
        } catch (RuntimeException e) {
            result.error("%s.expr does not compile: %s", name, e.getMessage());
            return;
        }

        JsonNode params = eval.path("params");
        if (!params.isArray() || params.isEmpty()) {
            result.error("%s.params must list the fields the expression reads", name);
            return;
        }

        for (JsonNode param : params) {
            String field = param.asText("");
            if (!expr.contains(field)) {
                result.warn("%s.params declares %s but the expression never reads it", name, field);
            }
        }
    }

    private void validateFlatMap(JsonNode flatMap, Result result) {
        if (flatMap.isMissingNode() || flatMap.isNull()) {
            return;
        }

        String pattern = flatMap.path("pattern").asText("");
        if (!pattern.isBlank()) {
            try {
                Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                result.error("flat_map.pattern is not a regex: %s", e.getDescription());
            }
        }

        Set<String> names = new HashSet<>();
        for (JsonNode field : flatMap.path("normal_fields")) {
            String name = field.path("name").asText("");
            String type = field.path("type").asText("");
            if (!NORMAL_FIELD_TYPES.contains(type)) {
                result.error(
                        "flat_map.normal_fields.%s.type must be one of %s, but got %s",
                        name, NORMAL_FIELD_TYPES, type);
            }

            if ("JSON".equals(type)) {
                // a json group is unpacked by json paths, the group itself carries no name
                for (JsonNode jsonPath : field.path("json_paths")) {
                    add(names, jsonPath.path("name").asText(""), "flat_map.normal_fields", result);
                    if (jsonPath.path("expr").asText("").isBlank()) {
                        result.error(
                                "flat_map.normal_fields.%s has no json path",
                                jsonPath.path("name").asText());
                    }
                    fieldType(jsonPath, "flat_map.normal_fields.json_paths", result);
                }
            } else {
                add(names, name, "flat_map.normal_fields", result);
            }
        }

        for (JsonNode field : flatMap.path("mapping_fields")) {
            String name = field.path("name").asText("");
            fieldType(field, "flat_map.mapping_fields." + name, result);

            String expr = field.path("expr").asText("");
            if (expr.isBlank()) {
                result.error("flat_map.mapping_fields.%s has no expr", name);
                continue;
            }
            try {
                AviatorEvaluator.compile(expr, true);
            } catch (RuntimeException e) {
                result.error(
                        "flat_map.mapping_fields.%s does not compile: %s", name, e.getMessage());
            }
        }
    }

    private void fieldType(JsonNode field, String where, Result result) {
        String type = field.path("type").asText("");
        if (!FIELD_TYPES.contains(type)) {
            result.error("%s.type must be one of %s, but got %s", where, FIELD_TYPES, type);
        }
    }

    private void add(Set<String> names, String name, String where, Result result) {
        if (name.isBlank()) {
            return;
        }
        if (!names.add(name)) {
            result.error("%s declares %s twice", where, name);
        }
    }

    private void validateKeys(JsonNode keys, Result result) {
        if (!keys.isArray() || keys.isEmpty()) {
            result.error("keys must not be empty, the aggregate groups the elements by them");
            return;
        }

        for (JsonNode key : keys) {
            known(key.asText(""), "keys", result);
        }
    }

    private void validateWindow(JsonNode window, Result result) {
        if (window.isMissingNode() || window.isNull()) {
            result.error("window is required when the job aggregates");
            return;
        }

        String type = window.path("type").asText("");
        if (!WINDOW_TYPES.contains(type)) {
            result.error("window.type must be one of %s, but got %s", WINDOW_TYPES, type);
        }

        String trigger = window.path("trigger").asText("");
        if (!TRIGGERS.contains(trigger)) {
            result.error("window.trigger must be one of %s, but got %s", TRIGGERS, trigger);
        }

        long size = window.path("size").asLong();
        long step = window.path("step").asLong();
        if (size <= 0) {
            result.error("window.size must be positive, but got %s", size);
        }
        if (window.path("offset").asLong() < 0) {
            result.error("window.offset must not be negative");
        }

        if ("SLIDE".equals(type) || "CUMULATE".equals(type)) {
            if (step <= 0) {
                result.error("a %s window needs a positive step, but got %s", type, step);
            } else if (size > 0 && size % step != 0) {
                // ! the engine slices a window by its step, the same restriction window TVFs have
                result.error(
                        "a %s window of %s ms cannot be sliced by a step of %s ms, the size must be"
                                + " an integral multiple of the step",
                        type, size, step);
            }
        }
    }

    private Set<String> validateAggregates(JsonNode aggregates, Result result) {
        Set<String> metrics = new HashSet<>();
        if (!aggregates.isArray() || aggregates.isEmpty()) {
            result.error("aggregates must not be empty when the job aggregates");
            return metrics;
        }

        for (JsonNode aggregate : aggregates) {
            String name = aggregate.path("name").asText("");
            if (name.isBlank()) {
                result.error("every aggregate needs a name, it is the metric of the alert");
            } else if (!metrics.add(name)) {
                result.error("aggregates declares the metric %s twice", name);
            }

            String method = aggregate.path("method").asText("");
            if (!METHODS.contains(method)) {
                result.error("aggregates.%s.method must be one of %s", name, METHODS);
            }

            JsonNode inputs = aggregate.path("inputs");
            if (!inputs.isArray() || inputs.isEmpty()) {
                // ! every method reads inputs[0], a COUNT without an input fails at runtime too
                result.error("aggregates.%s needs an input field", name);
            } else {
                known(inputs.get(0).asText(""), "aggregates." + name + ".inputs", result);
                if (inputs.size() > 1) {
                    result.warn(
                            "aggregates.%s declares %s inputs, the engine only reads the first one",
                            name, inputs.size());
                }
            }

            validateEval(aggregate.path("filter"), "aggregates." + name + ".filter", result);
        }

        return metrics;
    }

    private void validateThreshold(JsonNode threshold, Set<String> metrics, Result result) {
        if (threshold.isMissingNode() || threshold.isNull()) {
            result.error("threshold is required, a rule without it never alerts");
            return;
        }

        validateEval(threshold, "threshold", result);

        for (JsonNode param : threshold.path("params")) {
            String name = param.asText("");
            if (!name.isBlank() && !metrics.contains(name)) {
                result.error(
                        "threshold reads %s which no aggregate computes, the metrics are %s",
                        name, metrics);
            }
        }
    }

    /** A field the ETL operator does not emit can only be null at runtime. */
    private void known(String field, String where, Result result) {
        if (field.isBlank()) {
            result.error("%s contains a blank field name", where);
            return;
        }
        if (!schema.isEmpty() && !schema.contains(field)) {
            result.warn(
                    "%s reads %s which the etl output schema does not declare(%s)",
                    where, field, String.join(", ", schema.names()));
        }
    }

    /** The values a page offers, in the order they make sense in, not in the order of a set. */
    public static Map<String, List<String>> enums() {
        Map<String, List<String>> values = new LinkedHashMap<>();
        values.put("rule_state", List.of("ACTIVE", "UPDATE", "REMOVE"));
        values.put("window_type", List.of("TUMBLE", "SLIDE", "CUMULATE"));
        values.put("trigger", List.of("BATCH", "SINGLE"));
        values.put("method", List.of("COUNT", "COUNT_DISTINCT", "SUM", "AVG", "MAX", "MIN"));
        values.put("normal_field_type", List.of("STRING", "JSON"));
        values.put(
                "field_type",
                List.of("STRING", "BOOLEAN", "INT", "BIGINT", "FLOAT", "DOUBLE", "MAP", "LIST"));
        return values;
    }

    public static String normalize(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }
}
