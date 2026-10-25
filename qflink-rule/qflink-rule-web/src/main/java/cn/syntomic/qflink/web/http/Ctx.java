package cn.syntomic.qflink.web.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;

/** One request, with the path variables the route matched and the json helpers of a response. */
public class Ctx {

    private final HttpExchange exchange;
    private final Map<String, String> pathParams;
    private final Map<String, String> queryParams;

    public Ctx(HttpExchange exchange, Map<String, String> pathParams) {
        this.exchange = exchange;
        this.pathParams = pathParams;
        this.queryParams = parseQuery(exchange.getRequestURI().getRawQuery());
    }

    public HttpExchange exchange() {
        return exchange;
    }

    public String path(String name) {
        return pathParams.get(name);
    }

    public int pathInt(String name) {
        try {
            return Integer.parseInt(path(name));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer, but got " + path(name));
        }
    }

    public String query(String name, String defaultValue) {
        String value = queryParams.get(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /** Every query parameter, for a handler which passes them on as they came. */
    public Map<String, String> queries() {
        return queryParams;
    }

    public int queryInt(String name, int defaultValue) {
        String value = query(name, null);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    /** The request body as a json tree, an empty body is an empty object. */
    public JsonNode body() {
        try (InputStream in = exchange.getRequestBody()) {
            String raw = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return raw.isBlank() ? Json.object() : Json.read(raw);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read the request body", e);
        }
    }

    public ObjectNode objectBody() {
        JsonNode body = body();
        if (!body.isObject()) {
            throw new IllegalArgumentException("The request body must be a json object");
        }
        return (ObjectNode) body;
    }

    // -------------------------------------------------------------------------
    // Response
    // -------------------------------------------------------------------------

    public void json(int status, Object body) throws IOException {
        send(
                status,
                "application/json; charset=utf-8",
                Json.write(body).getBytes(StandardCharsets.UTF_8));
    }

    public void json(Object body) throws IOException {
        json(200, body);
    }

    public void noContent() throws IOException {
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    public void send(int status, String contentType, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> params = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return params;
        }

        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                params.put(decode(pair), "");
            } else {
                params.put(decode(pair.substring(0, eq)), decode(pair.substring(eq + 1)));
            }
        }
        return params;
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
