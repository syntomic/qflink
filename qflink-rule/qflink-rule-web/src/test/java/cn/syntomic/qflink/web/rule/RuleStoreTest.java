package cn.syntomic.qflink.web.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import cn.syntomic.qflink.web.http.Json;

class RuleStoreTest {

    @Test
    void aStoredRuleSurvivesARestart(@TempDir Path directory) {
        Path path = directory.resolve("rules.json");

        RuleStore store = new RuleStore(path);
        store.save(rule(7));
        store.markPublished(7, "ACTIVE");

        RuleStore reopened = new RuleStore(path);
        assertEquals(1, reopened.list().size());

        ObjectNode entry = reopened.get(7).orElseThrow();
        assertEquals("ACTIVE", entry.path("published_state").asText());
        assertEquals("a rule", entry.path("rule").path("extras").path("name").asText());
    }

    @Test
    void savingTwiceReplacesTheRuleAndKeepsItsPublication(@TempDir Path directory) {
        RuleStore store = new RuleStore(directory.resolve("rules.json"));
        store.save(rule(1));
        store.markPublished(1, "ACTIVE");

        ObjectNode updated = rule(1);
        ((ObjectNode) updated.get("extras")).put("name", "the same rule, edited");
        store.save(updated);

        assertEquals(1, store.list().size());
        ObjectNode entry = store.get(1).orElseThrow();
        assertEquals("ACTIVE", entry.path("published_state").asText());
        assertEquals(
                "the same rule, edited", entry.path("rule").path("extras").path("name").asText());
    }

    @Test
    void theNextIdFillsTheGaps(@TempDir Path directory) {
        RuleStore store = new RuleStore(directory.resolve("rules.json"));
        assertEquals(1, store.nextRuleId());

        store.save(rule(1));
        store.save(rule(3));
        assertEquals(2, store.nextRuleId());

        store.save(rule(2));
        assertEquals(4, store.nextRuleId());
    }

    @Test
    void deleteForgetsTheRule(@TempDir Path directory) {
        RuleStore store = new RuleStore(directory.resolve("rules.json"));
        store.save(rule(1));

        assertTrue(store.delete(1));
        assertFalse(store.delete(1));
        assertTrue(store.get(1).isEmpty());
    }

    private static ObjectNode rule(int ruleId) {
        ObjectNode rule = Json.of("rule_id", ruleId, "job_id", "test", "rule_state", "ACTIVE");
        rule.set("extras", Json.of("name", "a rule"));
        return rule;
    }
}
