package cn.syntomic.qflink.sql.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import cn.syntomic.qflink.sql.api.SqlTemplate.BoundSql;
import cn.syntomic.qflink.sql.api.SqlTemplate.Param;

/**
 * The smallest SQL2API: an api is a sql template plus its declared parameters, or a table plus the
 * fields the unified request may use. Parsing the request, generating the sql and wrapping the rows
 * into json are the platform's job, the developer only writes the definition.
 *
 * <pre>
 * GET  /api/movie/list?area=美国&amp;order=play_cnt%20DESC&amp;limit=20&amp;offset=0
 * POST /api/movie/query   with the unified request as its body
 * </pre>
 *
 * <p>An in-memory H2 database stands in for the OLAP engine a data service would query.
 */
public class Sql2Api {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DIR = "sql2api/";

    private final Connection connection;
    private final Map<String, SqlTemplate> templates = new HashMap<>();
    private final Map<String, QueryProtocol> protocols = new HashMap<>();

    public Sql2Api(Connection connection) {
        this.connection = connection;
    }

    /** Create the tables and register the apis of the resource directory. */
    public Sql2Api load() throws IOException, SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(resource("movie.sql"));
        }

        JsonNode list = JSON.readTree(resource("movie_list.json"));
        Map<String, Param> params = new LinkedHashMap<>();
        list.get("params")
                .fields()
                .forEachRemaining(
                        entry ->
                                params.put(
                                        entry.getKey(),
                                        JSON.convertValue(entry.getValue(), Param.class)));
        templates.put(
                list.get("path").asText(),
                new SqlTemplate(resource(list.get("sql").asText()), params));

        JsonNode query = JSON.readTree(resource("movie_query.json"));
        List<String> fields = new ArrayList<>();
        query.get("fields").forEach(field -> fields.add(field.asText()));
        protocols.put(
                query.get("path").asText(),
                new QueryProtocol(
                        query.get("table").asText(),
                        new LinkedHashSet<>(fields),
                        query.get("max_limit").asLong()));
        return this;
    }

    /** GET with query parameters, rendered by a template. */
    public Map<String, Object> get(String path, Map<String, String> request) throws SQLException {
        SqlTemplate template = templates.get(path);
        if (template == null) {
            return error(404, "No api " + path);
        }
        try {
            return ok(rows(template.render(request)), null);
        } catch (IllegalArgumentException e) {
            return error(400, e.getMessage());
        }
    }

    /** POST with the unified request as its body. */
    public Map<String, Object> post(String path, String body) throws SQLException {
        QueryProtocol protocol = protocols.get(path);
        if (protocol == null) {
            return error(404, "No api " + path);
        }
        try {
            QueryProtocol.Request request =
                    body.isBlank()
                            ? new QueryProtocol.Request()
                            : JSON.readValue(body, QueryProtocol.Request.class);
            List<BoundSql> sqls = protocol.build(request);
            Object total = rows(sqls.get(1)).get(0).get("total");
            return ok(rows(sqls.get(0)), total);
        } catch (IllegalArgumentException | IOException e) {
            return error(400, e.getMessage());
        }
    }

    private List<Map<String, Object>> rows(BoundSql bound) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(bound.sql)) {
            for (int i = 0; i < bound.args.size(); i++) {
                statement.setObject(i + 1, bound.args.get(i));
            }
            List<Map<String, Object>> rows = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                ResultSetMetaData meta = result.getMetaData();
                while (result.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        row.put(meta.getColumnLabel(i), result.getObject(i));
                    }
                    rows.add(row);
                }
            }
            return rows;
        }
    }

    private static Map<String, Object> ok(List<Map<String, Object>> data, Object total) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("code", 0);
        response.put("message", "success");
        response.put("data", data);
        if (total != null) {
            response.put("total", total);
        }
        return response;
    }

    private static Map<String, Object> error(int code, String message) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("code", code);
        response.put("message", message);
        return response;
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = Sql2Api.class.getClassLoader().getResourceAsStream(DIR + name)) {
            if (in == null) {
                throw new IOException("No resource " + DIR + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** An in-memory database whose column names stay in lower case, as the json keys. */
    public static Connection connect(String name) throws SQLException {
        return DriverManager.getConnection(
                "jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE");
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8090;
        Sql2Api api = new Sql2Api(connect("sql2api")).load();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext(
                "/api/",
                exchange -> {
                    try {
                        String path = exchange.getRequestURI().getPath();
                        Map<String, Object> response =
                                "POST".equals(exchange.getRequestMethod())
                                        ? api.post(path, body(exchange))
                                        : api.get(path, query(exchange));
                        reply(exchange, (int) response.get("code"), response);
                    } catch (SQLException e) {
                        reply(exchange, 500, error(500, e.getMessage()));
                    }
                });
        // ! one connection serves every request, a single thread keeps it from being shared
        server.setExecutor(null);
        server.start();
        System.out.printf(
                "SQL2API on http://127.0.0.1:%d%n"
                        + "  GET  /api/movie/list?area=美国&order=play_cnt%%20DESC&limit=20&offset=0%n"
                        + "  POST /api/movie/query%n",
                port);
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> params = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null) {
            return params;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                params.put(
                        URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return params;
    }

    private static String body(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void reply(HttpExchange exchange, int code, Object response) throws IOException {
        byte[] bytes = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(response);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(code == 0 ? 200 : code, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
