package cn.syntomic.qflink.web.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.web.http.Json;

/** The console must refuse what the engine would only reject at runtime. */
class RuleValidatorTest {

    private static final String SCHEMA =
            "{\"type\":\"record\",\"name\":\"default\",\"fields\":["
                    + "{\"name\":\"rule_id\",\"type\":\"int\"},{\"name\":\"key\",\"type\":\"string\"},"
                    + "{\"name\":\"key1\",\"type\":\"int\"},{\"name\":\"key2\",\"type\":\"string\"},"
                    + "{\"name\":\"is_odd\",\"type\":\"boolean\"}]}";

    private final RuleValidator validator = new RuleValidator("test", true, EtlSchema.of(SCHEMA));

    @Test
    void theExampleRuleIsValid() throws IOException {
        RuleValidator.Result result = validator.validate(example());

        assertTrue(result.isValid(), () -> "errors: " + result.getErrors());
        assertEquals(List.of(), result.getWarnings());
    }

    @Test
    void aWindowWhichCannotBeSlicedIsRefused() throws IOException {
        ObjectNode rule = example();
        ObjectNode window = (ObjectNode) rule.get("window");
        window.put("type", "SLIDE").put("size", 100_000).put("step", 30_000);

        assertContains(validator.validate(rule).getErrors(), "integral multiple of the step");
    }

    @Test
    void aSlideWindowWhoseSizeIsAMultipleOfItsStepIsFine() throws IOException {
        ObjectNode rule = example();
        ObjectNode window = (ObjectNode) rule.get("window");
        window.put("type", "SLIDE").put("size", 90_000).put("step", 30_000);

        assertTrue(validator.validate(rule).isValid());
    }

    @Test
    void anAggregateAlwaysNeedsAnInput() throws IOException {
        ObjectNode rule = example();
        // ! the engine reads inputs[0] whatever the method is, a COUNT without one fails too
        ((ObjectNode) rule.get("aggregates").get(0)).set("inputs", Json.array());

        assertContains(validator.validate(rule).getErrors(), "needs an input field");
    }

    @Test
    void aThresholdOnlyReadsTheMetricsTheAggregatesCompute() throws IOException {
        ObjectNode rule = example();
        ((ObjectNode) rule.get("threshold"))
                .put("expr", "missing > 1")
                .set("params", Json.ofStrings(List.of("missing")));

        assertContains(validator.validate(rule).getErrors(), "which no aggregate computes");
    }

    @Test
    void anExpressionWhichDoesNotCompileIsRefused() throws IOException {
        ObjectNode rule = example();
        ((ObjectNode) rule.get("filter")).put("expr", "raw ++ 1");

        assertContains(validator.validate(rule).getErrors(), "does not compile");
    }

    @Test
    void aRuleOfAnotherJobIsRefused() throws IOException {
        ObjectNode rule = example();
        rule.put("job_id", "another");

        assertContains(validator.validate(rule).getErrors(), "is not the job of this engine");
    }

    @Test
    void anUnknownFieldIsOnlyAWarning() throws IOException {
        ObjectNode rule = example();
        rule.set("keys", Json.ofStrings(List.of("not_in_the_schema")));

        RuleValidator.Result result = validator.validate(rule);
        assertTrue(result.isValid());
        assertContains(result.getWarnings(), "which the etl output schema does not declare");
    }

    @Test
    void aJobWhichDoesNotAggregateOnlyChecksTheEtl() throws IOException {
        ObjectNode rule = example();
        rule.remove(List.of("window", "keys", "aggregates", "threshold"));

        assertTrue(new RuleValidator("test", false, EtlSchema.of(SCHEMA)).validate(rule).isValid());
        assertFalse(validator.validate(rule).isValid());
    }

    private static void assertContains(List<String> messages, String fragment) {
        assertTrue(
                messages.stream().anyMatch(message -> message.contains(fragment)),
                () -> "no message contains '" + fragment + "', got " + messages);
    }

    /** The rule the module ships, the one the page offers. */
    private static ObjectNode example() throws IOException {
        try (InputStream in =
                RuleValidatorTest.class.getClassLoader().getResourceAsStream("example-rule.json")) {
            return (ObjectNode) Json.read(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}
