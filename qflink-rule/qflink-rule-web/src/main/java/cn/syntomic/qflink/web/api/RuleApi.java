package cn.syntomic.qflink.web.api;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.web.channel.Publisher;
import cn.syntomic.qflink.web.http.Ctx;
import cn.syntomic.qflink.web.http.EventBus;
import cn.syntomic.qflink.web.http.Json;
import cn.syntomic.qflink.web.http.Router;
import cn.syntomic.qflink.web.rule.RuleStore;
import cn.syntomic.qflink.web.rule.RuleValidator;

/**
 * The rules of the console.
 *
 * <p>Editing a rule only touches the store, publishing it appends a rule event to the channel the
 * engine reads. The two are kept apart on purpose: a draft which does not validate never reaches
 * the job, and a rule which is removed from the job is still there to be brought back.
 */
public class RuleApi {

    private static final Logger LOG = LoggerFactory.getLogger(RuleApi.class);

    private static final String ACTIVE = "ACTIVE";
    private static final String UPDATE = "UPDATE";
    private static final String REMOVE = "REMOVE";

    private final RuleStore store;
    private final RuleValidator validator;
    private final Publisher rules;
    private final EventBus events;
    private final String jobId;

    public RuleApi(
            RuleStore store,
            RuleValidator validator,
            Publisher rules,
            EventBus events,
            String jobId) {
        this.store = store;
        this.validator = validator;
        this.rules = rules;
        this.events = events;
        this.jobId = jobId;
    }

    /**
     * Bind the rule routes
     *
     * @param router
     */
    public void bind(Router router) {
        router.route("GET", "/api/rules", this::list)
                .route("POST", "/api/rules", this::save)
                .route("POST", "/api/rules/validate", this::validate)
                .route("POST", "/api/rules/publish-all", this::publishAll)
                .route("GET", "/api/rules/{id}", this::get)
                .route("PUT", "/api/rules/{id}", this::save)
                .route("DELETE", "/api/rules/{id}", this::delete)
                .route("POST", "/api/rules/{id}/publish", this::publish);
    }

    // -------------------------------------------------------------------------

    private void list(Ctx ctx) throws Exception {
        ArrayNode all = Json.array();
        store.list().forEach(all::add);
        ctx.json(all);
    }

    private void get(Ctx ctx) throws Exception {
        ObjectNode entry =
                store.get(ctx.pathInt("id"))
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "No rule " + ctx.path("id") + " in the store"));
        ctx.json(entry);
    }

    private void validate(Ctx ctx) throws Exception {
        ObjectNode rule = normalize(ctx.objectBody(), false);
        ctx.json(report(rule, validator.validate(rule)));
    }

    /**
     * Insert or replace a rule, {@code ?publish=true} sends it to the engine in the same call
     *
     * @param ctx
     * @throws Exception
     */
    private void save(Ctx ctx) throws Exception {
        ObjectNode rule = normalize(ctx.objectBody(), true);

        RuleValidator.Result result = validator.validate(rule);
        if (!result.isValid()) {
            ctx.json(422, report(rule, result));
            return;
        }

        ObjectNode entry = store.save(rule);
        if (Boolean.parseBoolean(ctx.query("publish", "false"))) {
            entry = send(rule.path(RuleStore.RULE_ID).asInt(), state(entry, null), false);
        }

        ObjectNode response = report(rule, result);
        response.set("entry", entry);
        ctx.json(response);
    }

    /**
     * Send a rule to the engine, the state defaults to ACTIVE the first time and UPDATE after that
     *
     * @param ctx
     * @throws Exception
     */
    private void publish(Ctx ctx) throws Exception {
        int ruleId = ctx.pathInt("id");
        JsonNode body = ctx.body();

        ObjectNode entry =
                store.get(ruleId)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "No rule " + ruleId + " in the store"));

        String state = RuleValidator.normalize(body.path("state").asText(null));
        boolean clearState = body.path("clear_state").asBoolean(false);

        ctx.json(send(ruleId, state(entry, state), clearState));
    }

    /**
     * Send every stored rule again, what a job which restarted with an empty channel needs
     *
     * @param ctx
     * @throws Exception
     */
    private void publishAll(Ctx ctx) throws Exception {
        List<ObjectNode> published = new ArrayList<>();
        for (ObjectNode entry : store.list()) {
            int ruleId = entry.path(RuleStore.RULE).path(RuleStore.RULE_ID).asInt();
            published.add(send(ruleId, state(entry, null), false));
        }

        ArrayNode all = Json.array();
        published.forEach(all::add);
        ctx.json(all);
    }

    /**
     * Remove a rule from the engine and from the store, {@code ?publish=false} only forgets it here
     *
     * @param ctx
     * @throws Exception
     */
    private void delete(Ctx ctx) throws Exception {
        int ruleId = ctx.pathInt("id");
        if (store.get(ruleId).isEmpty()) {
            throw new IllegalArgumentException("No rule " + ruleId + " in the store");
        }

        if (Boolean.parseBoolean(ctx.query("publish", "true"))) {
            send(ruleId, REMOVE, ctx.query("clear_state", "false").equals("true"));
        }
        store.delete(ruleId);

        ctx.json(Json.of("rule_id", ruleId, "deleted", true));
    }

    // -------------------------------------------------------------------------

    /**
     * Append one rule event to the channel the engine reads
     *
     * @param ruleId
     * @param state ACTIVE, UPDATE or REMOVE
     * @param clearState whether the engine drops the window state of the rule
     * @return the stored entry, with the publication it just got
     */
    private ObjectNode send(int ruleId, String state, boolean clearState) {
        ObjectNode entry =
                store.get(ruleId)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "No rule " + ruleId + " in the store"));

        ObjectNode rule = ((ObjectNode) entry.get(RuleStore.RULE)).deepCopy();
        rule.put(RuleStore.RULE_STATE, state);
        rule.put("clear_state", clearState);

        String record = Json.write(rule);
        rules.publish(record);
        store.markPublished(ruleId, state);
        LOG.info("Published rule {} as {} to {}", ruleId, state, rules.describe());

        events.publish(
                "rule",
                Json.of(
                        "rule_id", ruleId,
                        "state", state,
                        "clear_state", clearState,
                        "channel", rules.describe()));

        return store.get(ruleId).orElse(entry);
    }

    /** A rule reaches the engine as ACTIVE the first time and as UPDATE once it is known. */
    private String state(ObjectNode entry, String requested) {
        if (requested != null && !requested.isBlank()) {
            return requested;
        }

        String published = entry.path("published_state").asText("");
        return published.isBlank() || REMOVE.equals(published) ? ACTIVE : UPDATE;
    }

    /**
     * Fill in what the console owns: the job the engine runs and the id of a new rule
     *
     * @param rule
     * @param assignId whether a missing id is assigned
     * @return
     */
    private ObjectNode normalize(ObjectNode rule, boolean assignId) {
        if (assignId && !rule.path(RuleStore.RULE_ID).isInt()) {
            rule.put(RuleStore.RULE_ID, store.nextRuleId());
        }
        if (jobId != null && rule.path("job_id").asText("").isBlank()) {
            rule.put("job_id", jobId);
        }
        if (rule.path(RuleStore.RULE_STATE).asText("").isBlank()) {
            rule.put(RuleStore.RULE_STATE, ACTIVE);
        }
        return rule;
    }

    private ObjectNode report(ObjectNode rule, RuleValidator.Result result) {
        ObjectNode report = Json.of("valid", result.isValid());
        report.set("errors", Json.ofStrings(result.getErrors()));
        report.set("warnings", Json.ofStrings(result.getWarnings()));
        report.set("rule", rule);
        return report;
    }
}
