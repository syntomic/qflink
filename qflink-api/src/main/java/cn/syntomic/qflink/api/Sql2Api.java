package cn.syntomic.qflink.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import cn.syntomic.qflink.api.SqlTemplate.BoundSql;
import cn.syntomic.qflink.api.SqlTemplate.Param;

/**
 * The smallest SQL2API: an api is a sql template plus its declared parameters, or a table plus the
 * fields the unified request may use. Parsing the request, generating the sql and wrapping the rows
 * into json are the platform's job, the developer only writes the definition.
 *
 * <p>The definitions are the files of one directory, read when the apis are loaded:
 *
 * <ul>
 *   <li>{@code schema.sql}, optional: the statements which create the tables, run first
 *   <li>{@code <name>.json} with {@code sql}: a template api, called with GET and query parameters
 *   <li>{@code <name>.json} with {@code table}: a unified request api, called with POST and a body
 * </ul>
 *
 * <p>The paths of the definitions are relative, the server decides where they are mounted: the
 * standalone server below mounts them under {@code /api}, another server may embed the library and
 * mount them anywhere.
 */
public class Sql2Api {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Where the definitions of this module are, relative to the repository root. */
    public static final String DEFAULT_DIR = "qflink-api/src/main/resources/sql2api";

    /** Hands out a connection per request, so concurrent requests never share one. */
    @FunctionalInterface
    public interface Connections {
        Connection get() throws SQLException;
    }

    private final Connections connections;
    private final Map<String, SqlTemplate> templates = new LinkedHashMap<>();
    private final Map<String, QueryProtocol> protocols = new LinkedHashMap<>();
    private final List<Map<String, Object>> described = new ArrayList<>();

    public Sql2Api(Connections connections) {
        this.connections = connections;
    }

    /**
     * Create the tables of {@code schema.sql} and register every api definition of a directory
     *
     * @param dir the directory of the definitions
     * @return this
     */
    public Sql2Api load(Path dir) throws IOException, SQLException {
        if (!Files.isDirectory(dir)) {
            throw new IOException("No api definition directory " + dir.toAbsolutePath());
        }

        Path schema = dir.resolve("schema.sql");
        if (Files.isRegularFile(schema)) {
            try (Connection connection = connections.get();
                    Statement statement = connection.createStatement()) {
                statement.execute(Files.readString(schema));
            }
        }

        List<Path> definitions;
        try (Stream<Path> files = Files.list(dir)) {
            definitions = files.filter(f -> f.toString().endsWith(".json")).sorted().toList();
        }
        for (Path definition : definitions) {
            register(dir, definition);
        }
        return this;
    }

    private void register(Path dir, Path file) throws IOException {
        JsonNode definition = JSON.readTree(file.toFile());
        String path = definition.path("path").asText();
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException(file + ": the path must start with /, got " + path);
        }

        Map<String, Object> api = new LinkedHashMap<>();
        api.put("name", file.getFileName().toString().replaceFirst("\\.json$", ""));
        api.put("path", path);
        api.put("description", definition.path("description").asText(""));

        if (definition.has("sql")) {
            String sql = Files.readString(dir.resolve(definition.get("sql").asText()));
            Map<String, Param> params = new LinkedHashMap<>();
            definition
                    .path("params")
                    .fields()
                    .forEachRemaining(
                            entry ->
                                    params.put(
                                            entry.getKey(),
                                            JSON.convertValue(entry.getValue(), Param.class)));
            templates.put(path, new SqlTemplate(sql, params));
            api.put("method", "GET");
            api.put("kind", "template");
            api.put("sql", sql);
            api.put("params", definition.path("params"));
        } else if (definition.has("table")) {
            List<String> fields = new ArrayList<>();
            definition.get("fields").forEach(field -> fields.add(field.asText()));
            long maxLimit = definition.path("max_limit").asLong(100);
            protocols.put(
                    path,
                    new QueryProtocol(
                            definition.get("table").asText(),
                            new LinkedHashSet<>(fields),
                            maxLimit));
            api.put("method", "POST");
            api.put("kind", "query");
            api.put("table", definition.get("table").asText());
            api.put("fields", fields);
            api.put("max_limit", maxLimit);
        } else {
            throw new IllegalArgumentException(file + ": an api needs either sql or table");
        }
        described.add(api);
    }

    /** What every registered api is and how it is called, for a page to list them. */
    public List<Map<String, Object>> describe() {
        return described;
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
        try (Connection connection = connections.get();
                PreparedStatement statement = connection.prepareStatement(bound.sql)) {
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

    /**
     * An H2 database whose column names stay in lower case, as the json keys
     *
     * @param url what follows {@code jdbc:h2:}, e.g. {@code mem:sql2api} or {@code file:/tmp/db}
     */
    public static Connections h2(String url) {
        String jdbc = "jdbc:h2:" + url + ";DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE";
        return () -> DriverManager.getConnection(jdbc);
    }

    /**
     * Serve the definitions of a directory on their own
     *
     * <pre>{@code --dir <definitions> --port 8090}</pre>
     */
    public static void main(String[] args) throws Exception {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            options.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        int port = Integer.parseInt(options.getOrDefault("port", "8090"));
        Path dir = Path.of(options.getOrDefault("dir", DEFAULT_DIR));
        Sql2Api api = new Sql2Api(h2("mem:sql2api")).load(dir);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext(
                "/api/",
                exchange -> {
                    try {
                        String path = exchange.getRequestURI().getPath().substring("/api".length());
                        Map<String, Object> response =
                                "POST".equals(exchange.getRequestMethod())
                                        ? api.post(path, body(exchange))
                                        : api.get(path, query(exchange));
                        reply(exchange, (int) response.get("code"), response);
                    } catch (SQLException e) {
                        reply(exchange, 500, error(500, e.getMessage()));
                    }
                });
        server.start();
        System.out.printf("SQL2API on http://127.0.0.1:%d, definitions in %s%n", port, dir);
        for (Map<String, Object> described : api.describe()) {
            System.out.printf("  %-4s /api%s%n", described.get("method"), described.get("path"));
        }
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
