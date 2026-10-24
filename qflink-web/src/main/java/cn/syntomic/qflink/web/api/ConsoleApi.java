package cn.syntomic.qflink.web.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import cn.syntomic.qflink.web.channel.Channels;
import cn.syntomic.qflink.web.configuration.Durations;
import cn.syntomic.qflink.web.configuration.WebConf;
import cn.syntomic.qflink.web.configuration.WebOptions;
import cn.syntomic.qflink.web.generator.LogGenerator;
import cn.syntomic.qflink.web.http.Ctx;
import cn.syntomic.qflink.web.http.EventBus;
import cn.syntomic.qflink.web.http.Json;
import cn.syntomic.qflink.web.http.Router;
import cn.syntomic.qflink.web.rule.EtlSchema;
import cn.syntomic.qflink.web.rule.RuleValidator;

/**
 * What the page needs besides the rules: the shape of the job it drives, the events it already
 * missed, and the log generator.
 */
public class ConsoleApi {

    private static final int DEFAULT_HISTORY_LIMIT = 200;
    private static final String EXAMPLE_RULE = "example-rule.json";

    private final WebConf conf;
    private final EtlSchema schema;
    private final EventBus events;
    private final LogGenerator generator;
    private final Map<String, String> channels;

    public ConsoleApi(
            WebConf conf,
            EtlSchema schema,
            EventBus events,
            LogGenerator generator,
            Map<String, String> channels) {
        this.conf = conf;
        this.schema = schema;
        this.events = events;
        this.generator = generator;
        this.channels = channels;
    }

    /**
     * Bind the console routes
     *
     * @param router
     */
    public void bind(Router router) {
        router.route("GET", "/api/meta", this::meta)
                .route("GET", "/api/status", this::status)
                .route("GET", "/api/history", this::history)
                .route("POST", "/api/logs", this::logs)
                .route("GET", "/api/generator", this::generatorStatus)
                .route("POST", "/api/generator/start", this::generatorStart)
                .route("POST", "/api/generator/stop", this::generatorStop);
    }

    // -------------------------------------------------------------------------

    /** Everything the page needs once, at load. */
    private void meta(Ctx ctx) throws Exception {
        ObjectNode meta =
                Json.of(
                        "job_id", conf.get(WebOptions.JOB_ID, "unknown"),
                        "agg_enabled", conf.getBoolean("agg.enable", true),
                        "log_format", conf.get(WebOptions.LOG_FORMAT, "JSON"),
                        "log_field_time", conf.get(WebOptions.LOG_FIELD_TIME, ""));

        ObjectNode enums = Json.object();
        RuleValidator.enums().forEach((name, values) -> enums.set(name, Json.ofStrings(values)));
        meta.set("enums", enums);

        // ! a qfile source reads one line every scan-interval, a generator which runs faster than
        // ! that only piles the lines up in the file, the page says so instead of looking stuck
        meta.put("log_source", conf.get(WebOptions.LOG, WebOptions.SOURCE, Channels.QFILE));
        meta.put(
                "log_scan_interval_ms",
                Durations.toMillis(conf.get(WebOptions.LOG, "scan-interval", null), 2000L));

        meta.set("fields", Json.ofMap(schema.toMap()));
        meta.set("channels", Json.ofMap(channels));
        meta.set("example_rule", example());
        meta.put("generator_template", conf.get(WebOptions.GENERATOR_TEMPLATE, ""));
        ctx.json(meta);
    }

    /** The rule the module ships, what the editor starts from when the store is empty. */
    private JsonNode example() {
        try (InputStream in = ConsoleApi.class.getClassLoader().getResourceAsStream(EXAMPLE_RULE)) {
            return in == null
                    ? Json.object()
                    : Json.read(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            return Json.object();
        }
    }

    private void status(Ctx ctx) throws Exception {
        ObjectNode status = Json.of("open_pages", events.openPages());
        status.set("totals", Json.mapper().valueToTree(events.totals()));
        status.set("generator", generator.status());
        status.set("channels", Json.ofMap(channels));
        ctx.json(status);
    }

    /**
     * The events a page which just opened missed, {@code ?type=alert&limit=200}
     *
     * @param ctx
     * @throws Exception
     */
    private void history(Ctx ctx) throws Exception {
        String type = ctx.query("type", "alert");
        int limit = ctx.queryInt("limit", DEFAULT_HISTORY_LIMIT);

        ArrayNode all = Json.array();
        events.history(type, limit).forEach(all::add);
        ctx.json(all);
    }

    /**
     * Send one line or a few, {@code {"line": "..."}} or {@code {"lines": ["...", "..."]}}
     *
     * @param ctx
     * @throws Exception
     */
    private void logs(Ctx ctx) throws Exception {
        JsonNode body = ctx.body();

        List<String> lines = new ArrayList<>();
        if (body.hasNonNull("line")) {
            lines.add(body.get("line").asText());
        }
        for (JsonNode line : body.path("lines")) {
            lines.add(line.asText());
        }
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Neither line nor lines was given");
        }

        ArrayNode published = Json.array();
        lines.forEach(line -> published.add(generator.send(line)));
        ctx.json(Json.of("published", published));
    }

    private void generatorStatus(Ctx ctx) throws Exception {
        ctx.json(generator.status());
    }

    private void generatorStart(Ctx ctx) throws Exception {
        JsonNode body = ctx.body();
        String template = body.path("template").asText(conf.get(WebOptions.GENERATOR_TEMPLATE, ""));
        if (template.isBlank()) {
            throw new IllegalArgumentException(
                    "No log template, set one on the page or in " + WebOptions.GENERATOR_TEMPLATE);
        }

        generator.start(template, body.path("rate").asDouble(1.0d));
        ctx.json(generator.status());
    }

    private void generatorStop(Ctx ctx) throws Exception {
        generator.stop();
        ctx.json(generator.status());
    }
}
