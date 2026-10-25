package cn.syntomic.qflink.web;

import static cn.syntomic.qflink.web.configuration.WebOptions.ALERT;
import static cn.syntomic.qflink.web.configuration.WebOptions.DATA_DIR;
import static cn.syntomic.qflink.web.configuration.WebOptions.ERROR;
import static cn.syntomic.qflink.web.configuration.WebOptions.ETL_OUTPUT_SCHEMA;
import static cn.syntomic.qflink.web.configuration.WebOptions.GENERATOR_TEMPLATE;
import static cn.syntomic.qflink.web.configuration.WebOptions.HISTORY_SIZE;
import static cn.syntomic.qflink.web.configuration.WebOptions.JOB_ID;
import static cn.syntomic.qflink.web.configuration.WebOptions.PORT;
import static cn.syntomic.qflink.web.configuration.WebOptions.RULE;
import static cn.syntomic.qflink.web.configuration.WebOptions.STATIC_DIR;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.web.api.ConsoleApi;
import cn.syntomic.qflink.web.api.RuleApi;
import cn.syntomic.qflink.web.channel.Channels;
import cn.syntomic.qflink.web.channel.Publisher;
import cn.syntomic.qflink.web.channel.Subscriber;
import cn.syntomic.qflink.web.configuration.WebConf;
import cn.syntomic.qflink.web.configuration.WebOptions;
import cn.syntomic.qflink.web.generator.LogGenerator;
import cn.syntomic.qflink.web.http.EventBus;
import cn.syntomic.qflink.web.http.Json;
import cn.syntomic.qflink.web.http.Router;
import cn.syntomic.qflink.web.http.StaticHandler;
import cn.syntomic.qflink.web.rule.EtlSchema;
import cn.syntomic.qflink.web.rule.RuleStore;
import cn.syntomic.qflink.web.rule.RuleValidator;

/**
 * The console of the rule engine: it writes the rules the job reads and reads the alerts the job
 * writes.
 *
 * <p>It is a plain java process, it needs neither flink nor a cluster: the job is driven through
 * its own channels, so the same console works against a local run reading files and against a
 * cluster reading kafka.
 */
public class WebServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);

    private static final int DEFAULT_PORT = 8080;
    private static final int DEFAULT_HISTORY_SIZE = 500;

    private final WebConf conf;
    private final EventBus events;
    private final List<AutoCloseable> resources = new ArrayList<>();

    private HttpServer server;
    private ExecutorService executor;

    public WebServer(WebConf conf) {
        this.conf = conf;
        this.events = new EventBus(conf.getInt(HISTORY_SIZE, DEFAULT_HISTORY_SIZE));
    }

    public static void main(String[] args) throws Exception {
        WebConf conf = WebConf.load(args);

        WebServer server = new WebServer(conf);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "qflink-web-shutdown"));
        server.start();
    }

    /**
     * Wire the channels, the store and the routes, then serve
     *
     * @throws IOException
     */
    public void start() throws IOException {
        Channels channels = new Channels(conf);
        Map<String, String> described = new LinkedHashMap<>();

        Publisher rules = register(channels.publisher(RULE));
        Publisher logs = register(channels.publisher(WebOptions.LOG));
        described.put(RULE, rules.describe());
        described.put(WebOptions.LOG, logs.describe());

        subscribe(channels, ALERT, described);
        subscribe(channels, ERROR, described);

        String jobId = conf.get(JOB_ID);
        EtlSchema schema = EtlSchema.of(conf.get(ETL_OUTPUT_SCHEMA));
        RuleStore store =
                new RuleStore(
                        Path.of(
                                conf.get(DATA_DIR, "qflink-rule/qflink-rule-web/data"),
                                "rules.json"));
        RuleValidator validator =
                new RuleValidator(jobId, conf.getBoolean("agg.enable", true), schema);
        LogGenerator generator =
                register(new LogGenerator(logs, events, conf.get(GENERATOR_TEMPLATE, "")));

        Router router = new Router();
        new RuleApi(store, validator, rules, events, jobId).bind(router);
        new ConsoleApi(conf, schema, events, generator, described).bind(router);
        router.route("GET", "/api/events", ctx -> events.handler().handle(ctx.exchange()))
                .fallback(new StaticHandler(conf.get(STATIC_DIR)));

        int port = conf.getInt(PORT, DEFAULT_PORT);
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", router);
        server.setExecutor(executor);
        server.start();

        LOG.info("Rule Engine Console: http://localhost:{}", port);
        LOG.info("Rule Engine Console: job {}, channels {}", jobId, described);
        LOG.info("Rule Engine Console: {} rules in {}", store.list().size(), conf.get(DATA_DIR));
    }

    /**
     * Read what a job sink writes and push it to the open pages
     *
     * @param channels
     * @param name alert or error
     * @param described the channels shown on the overview page
     */
    private void subscribe(Channels channels, String name, Map<String, String> described) {
        Subscriber subscriber = channels.subscriber(name);
        if (subscriber == null) {
            // ! a sink which only prints has nothing to read, the page says so instead of staying
            // empty
            described.put(name, "print, nothing to read");
            LOG.warn(
                    "The {} sink of the job is print, the console cannot show it, set {}.sink to"
                            + " qfile or kafka",
                    name,
                    name);
            return;
        }

        described.put(name, subscriber.describe());
        register(subscriber);
        subscriber.start(record -> events.publish(name, parse(record)));
    }

    /** An alert is json, anything else is pushed as it came so nothing is silently dropped. */
    private JsonNode parse(String record) {
        try {
            return Json.read(record);
        } catch (RuntimeException e) {
            return Json.of("raw", record);
        }
    }

    private <T extends AutoCloseable> T register(T resource) {
        resources.add(resource);
        return resource;
    }

    @Override
    public void close() {
        LOG.info("Rule Engine Console: stopping");

        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            } catch (Exception e) {
                LOG.warn("Cannot close {}", resource.getClass().getSimpleName(), e);
            }
        }
        resources.clear();

        events.close();
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
