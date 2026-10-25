package cn.syntomic.qflink.web.generator;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.web.channel.Publisher;
import cn.syntomic.qflink.web.http.EventBus;
import cn.syntomic.qflink.web.http.Json;

/**
 * Feeds the log channel at a chosen rate, the counterpart of the transaction generator of a fraud
 * detection demo: a rule is only worth looking at once something flows through the job.
 */
public class LogGenerator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LogGenerator.class);

    /** At most five sampled lines per second reach the page, whatever the rate is. */
    private static final long SAMPLE_INTERVAL_MS = 200L;

    private final Publisher logs;
    private final EventBus events;
    private final Template template = new Template();

    private final AtomicLong sent = new AtomicLong();

    private volatile boolean running;
    private volatile String line;
    private volatile double rate;
    private volatile String startedAt;
    private volatile String lastError;

    private Thread thread;
    private long lastSampledAt;

    public LogGenerator(Publisher logs, EventBus events, String defaultTemplate) {
        this.logs = logs;
        this.events = events;
        this.line = defaultTemplate;
        this.rate = 1.0d;
    }

    /**
     * Start to generate, restarting with the new settings if it already runs
     *
     * @param template the log line, {@code ${...}} placeholders are expanded per line
     * @param rate lines per second
     */
    public synchronized void start(String template, double rate) {
        if (rate <= 0 || rate > 10_000) {
            throw new IllegalArgumentException(
                    "The rate must be within (0, 10000], but got " + rate);
        }

        // ! fail before the thread starts, a broken template would only log on the server otherwise
        String sample = this.template.expand(template);
        LOG.info("The generator starts at {} lines/s, e.g. {}", rate, sample);

        stop();
        this.line = template;
        this.rate = rate;
        this.lastError = null;
        this.running = true;
        this.startedAt = Instant.now().toString();

        thread = new Thread(this::generate, "qflink-web-generator");
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    /**
     * Send one line, the manual counterpart of the generator
     *
     * @param raw an already expanded line, or a template
     * @return what was published
     */
    public String send(String raw) {
        String expanded = template.expand(raw);
        logs.publish(expanded);
        sent.incrementAndGet();
        events.publish("log", Json.of("line", expanded, "source", "manual"));
        return expanded;
    }

    private void generate() {
        long intervalNanos = (long) (1_000_000_000L / rate);

        while (running) {
            long start = System.nanoTime();
            try {
                String expanded = template.expand(line);
                logs.publish(expanded);
                sent.incrementAndGet();
                sample(expanded);
            } catch (RuntimeException e) {
                lastError = e.getMessage();
                LOG.warn("The generator stopped on a broken line", e);
                running = false;
                events.publish("status", Json.of("generator", "stopped", "error", lastError));
                return;
            }

            long elapsed = System.nanoTime() - start;
            long sleep = intervalNanos - elapsed;
            if (sleep > 0) {
                try {
                    Thread.sleep(sleep / 1_000_000L, (int) (sleep % 1_000_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** A high rate must not flood the page, only a line every now and then is pushed. */
    private void sample(String expanded) {
        long now = System.currentTimeMillis();
        if (now - lastSampledAt < SAMPLE_INTERVAL_MS) {
            return;
        }

        lastSampledAt = now;
        events.publish("log", Json.of("line", expanded, "source", "generator", "sent", sent.get()));
    }

    public ObjectNode status() {
        ObjectNode status =
                Json.of(
                        "running", running,
                        "rate", rate,
                        "template", line,
                        "sent", sent.get());
        status.put("started_at", startedAt);
        status.put("error", lastError);
        status.put("channel", logs.describe());
        return status;
    }

    @Override
    public void close() {
        stop();
    }
}
