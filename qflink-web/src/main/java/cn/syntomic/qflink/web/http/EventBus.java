package cn.syntomic.qflink.web.http;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fans the alerts, the errors and the logs out to every open page and keeps the last ones, so a
 * page which opens in the middle of a run is not empty.
 *
 * <p>Server sent events, not a websocket: the console only ever pushes, the page posts.
 */
public class EventBus implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EventBus.class);

    private static final long HEARTBEAT_SECONDS = 15L;

    /** The name of every server sent event, see {@link #frame(String, ObjectNode)}. */
    private static final String EVENT_NAME = "qflink";

    private final int historySize;
    private final Map<String, Deque<ObjectNode>> history = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> totals = new ConcurrentHashMap<>();
    private final List<Client> clients = new CopyOnWriteArrayList<>();
    private final AtomicLong sequence = new AtomicLong();
    private final ScheduledExecutorService heartbeat;

    public EventBus(int historySize) {
        this.historySize = historySize;
        this.heartbeat =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "qflink-web-heartbeat");
                            thread.setDaemon(true);
                            return thread;
                        });

        // ! a proxy or a laptop which slept closes a silent connection, the page reconnects on its
        // own
        heartbeat.scheduleAtFixedRate(
                () -> broadcast(": ping\n\n"),
                HEARTBEAT_SECONDS,
                HEARTBEAT_SECONDS,
                TimeUnit.SECONDS);
    }

    /**
     * Publish one event to the open pages and to the history
     *
     * @param type alert, error, log, rule or status
     * @param payload
     */
    public void publish(String type, JsonNode payload) {
        ObjectNode event =
                Json.of(
                        "seq", sequence.incrementAndGet(),
                        "type", type,
                        "time", Instant.now().toString());
        event.set("payload", payload);

        totals.computeIfAbsent(type, key -> new AtomicLong()).incrementAndGet();

        Deque<ObjectNode> recent = history.computeIfAbsent(type, key -> new ArrayDeque<>());
        synchronized (recent) {
            recent.addLast(event);
            while (recent.size() > historySize) {
                recent.removeFirst();
            }
        }

        broadcast(frame(type, event));
    }

    /**
     * The last events of a type, oldest first
     *
     * @param type
     * @param limit
     * @return
     */
    public List<ObjectNode> history(String type, int limit) {
        Deque<ObjectNode> recent = history.get(type);
        if (recent == null) {
            return List.of();
        }

        synchronized (recent) {
            List<ObjectNode> all = new ArrayList<>(recent);
            return all.size() <= limit ? all : all.subList(all.size() - limit, all.size());
        }
    }

    /** How many events of every type the console saw since it started. */
    public Map<String, Long> totals() {
        Map<String, Long> counts = new java.util.LinkedHashMap<>();
        totals.forEach((type, count) -> counts.put(type, count.get()));
        return counts;
    }

    public int openPages() {
        return clients.size();
    }

    /** The endpoint a page keeps open, every event is written to it as it happens. */
    public HttpHandler handler() {
        return exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache, no-transform");
            exchange.getResponseHeaders().set("Connection", "keep-alive");
            // ! no content length, the response is open until the page goes away
            exchange.sendResponseHeaders(200, 0);

            Client client = new Client(exchange);
            clients.add(client);
            client.write(": connected\n\n");
            LOG.info("A page opened the event stream, {} open", clients.size());
        };
    }

    private void broadcast(String frame) {
        for (Client client : clients) {
            if (!client.write(frame)) {
                clients.remove(client);
                LOG.info("A page closed the event stream, {} open", clients.size());
            }
        }
    }

    /**
     * One event name for every type, the page dispatches on the type of the payload.
     *
     * <p>! an {@code event: error} frame would reach the page as the failure event of its own event
     * source, there is no telling the two apart there.
     */
    private static String frame(String type, ObjectNode event) {
        return "event: " + EVENT_NAME + "\ndata: " + Json.write(event) + "\n\n";
    }

    @Override
    public void close() {
        heartbeat.shutdownNow();
        clients.forEach(Client::close);
        clients.clear();
    }

    /** One open page. */
    private static class Client {

        private final HttpExchange exchange;
        private final OutputStream out;

        private Client(HttpExchange exchange) {
            this.exchange = exchange;
            this.out = exchange.getResponseBody();
        }

        private synchronized boolean write(String frame) {
            try {
                out.write(frame.getBytes(StandardCharsets.UTF_8));
                out.flush();
                return true;
            } catch (IOException e) {
                close();
                return false;
            }
        }

        private void close() {
            exchange.close();
        }
    }
}
