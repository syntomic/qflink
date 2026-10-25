package cn.syntomic.qflink.web.rule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import cn.syntomic.qflink.web.http.Json;

/**
 * The rules the console manages, kept in one json file.
 *
 * <p>The channel the rule engine reads is an append only stream of rule events, it is not a list
 * the console could read back: the store is what the page lists and edits, the channel is what the
 * job replays. A rule is only in the job once it has been published.
 */
public class RuleStore {

    public static final String RULE = "rule";
    public static final String RULE_ID = "rule_id";
    public static final String RULE_STATE = "rule_state";

    private static final String UPDATED_AT = "updated_at";
    private static final String PUBLISHED_AT = "published_at";
    private static final String PUBLISHED_STATE = "published_state";

    private final Path path;

    /** rule id to the stored entry, ordered by insertion so the page keeps a stable list. */
    private final Map<Integer, ObjectNode> entries = new LinkedHashMap<>();

    public RuleStore(Path path) {
        this.path = path;
        load();
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    public synchronized List<ObjectNode> list() {
        return new ArrayList<>(entries.values());
    }

    public synchronized Optional<ObjectNode> get(int ruleId) {
        return Optional.ofNullable(entries.get(ruleId));
    }

    /** The smallest positive id no rule uses yet. */
    public synchronized int nextRuleId() {
        int candidate = 1;
        while (entries.containsKey(candidate)) {
            candidate++;
        }
        return candidate;
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Insert or replace a rule, the stored one keeps its publication state
     *
     * @param rule the rule document, {@code rule_id} must be set
     * @return the stored entry
     */
    public synchronized ObjectNode save(ObjectNode rule) {
        int ruleId = rule.path(RULE_ID).asInt();

        ObjectNode entry = entries.get(ruleId);
        if (entry == null) {
            entry = Json.object();
            entry.putNull(PUBLISHED_AT);
            entry.putNull(PUBLISHED_STATE);
        }

        entry.set(RULE, rule);
        entry.put(UPDATED_AT, Instant.now().toString());
        entries.put(ruleId, entry);

        flush();
        return entry;
    }

    /**
     * Record that a rule reached the channel the job reads
     *
     * @param ruleId
     * @param state the rule state the job received, ACTIVE, UPDATE or REMOVE
     */
    public synchronized void markPublished(int ruleId, String state) {
        ObjectNode entry = entries.get(ruleId);
        if (entry == null) {
            return;
        }

        entry.put(PUBLISHED_AT, Instant.now().toString());
        entry.put(PUBLISHED_STATE, state);
        flush();
    }

    public synchronized boolean delete(int ruleId) {
        boolean removed = entries.remove(ruleId) != null;
        if (removed) {
            flush();
        }
        return removed;
    }

    // -------------------------------------------------------------------------
    // Persistence
    // -------------------------------------------------------------------------

    private void load() {
        if (!Files.isReadable(path)) {
            return;
        }

        try {
            String content = Files.readString(path, StandardCharsets.UTF_8);
            if (content.isBlank()) {
                return;
            }

            for (JsonNode entry : Json.read(content)) {
                int ruleId = entry.path(RULE).path(RULE_ID).asInt();
                entries.put(ruleId, (ObjectNode) entry);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the rule store " + path, e);
        }
    }

    private void flush() {
        ArrayNode all = Json.array();
        entries.values().forEach(all::add);

        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }

            // ! write beside the store and move, a half written file would lose every rule
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, Json.writePretty(all), StandardCharsets.UTF_8);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write the rule store " + path, e);
        }
    }
}
