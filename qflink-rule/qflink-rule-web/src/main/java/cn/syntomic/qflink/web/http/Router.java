package cn.syntomic.qflink.web.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A few dozen lines of routing over the jdk http server, the console needs nothing more. */
public class Router implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Router.class);

    private static final Pattern VARIABLE = Pattern.compile("\\{([a-zA-Z0-9_]+)\\}");

    /** What a route does with a request. */
    public interface Handler {
        void handle(Ctx ctx) throws Exception;
    }

    private record Route(String method, Pattern pattern, List<String> variables, Handler handler) {}

    private final List<Route> routes = new ArrayList<>();
    private HttpHandler fallback;

    /**
     * Register a route, {@code /api/rules/{id}} binds the id path variable
     *
     * @param method
     * @param path
     * @param handler
     * @return this
     */
    public Router route(String method, String path, Handler handler) {
        List<String> variables = new ArrayList<>();
        StringBuilder regex = new StringBuilder("^");

        Matcher matcher = VARIABLE.matcher(path);
        int last = 0;
        while (matcher.find()) {
            regex.append(Pattern.quote(path.substring(last, matcher.start()))).append("([^/]+)");
            variables.add(matcher.group(1));
            last = matcher.end();
        }
        regex.append(Pattern.quote(path.substring(last))).append("$");

        routes.add(new Route(method, Pattern.compile(regex.toString()), variables, handler));
        return this;
    }

    /** Everything which matched no route, the static page. */
    public Router fallback(HttpHandler handler) {
        this.fallback = handler;
        return this;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();

        for (Route route : routes) {
            Matcher matcher = route.pattern().matcher(path);
            if (!matcher.matches()) {
                continue;
            }
            if (!route.method().equalsIgnoreCase(method)) {
                continue;
            }

            Map<String, String> params = new LinkedHashMap<>();
            for (int i = 0; i < route.variables().size(); i++) {
                params.put(route.variables().get(i), matcher.group(i + 1));
            }

            dispatch(route, new Ctx(exchange, params));
            return;
        }

        if (fallback != null) {
            fallback.handle(exchange);
            return;
        }

        new Ctx(exchange, Map.of())
                .json(404, Json.of("error", "No route for " + method + " " + path));
    }

    private void dispatch(Route route, Ctx ctx) throws IOException {
        try {
            route.handler().handle(ctx);
        } catch (IllegalArgumentException | IllegalStateException e) {
            LOG.debug("Rejected {}", ctx.exchange().getRequestURI(), e);
            ctx.json(400, Json.of("error", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            LOG.error("Failed to handle {}", ctx.exchange().getRequestURI(), e);
            ctx.json(500, Json.of("error", e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }
}
