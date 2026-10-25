package cn.syntomic.qflink.web.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * Serves the page, from the classpath or from a directory.
 *
 * <p>Point {@code web.static-dir} at the source directory while working on the page, the console
 * then reads every file per request and a reload is enough to see a change.
 */
public class StaticHandler implements HttpHandler {

    private static final String CLASSPATH_DIR = "web";
    private static final String INDEX = "index.html";

    private static final Map<String, String> CONTENT_TYPES =
            Map.of(
                    "html", "text/html; charset=utf-8",
                    "js", "text/javascript; charset=utf-8",
                    "css", "text/css; charset=utf-8",
                    "json", "application/json; charset=utf-8",
                    "svg", "image/svg+xml",
                    "ico", "image/x-icon");

    private final Path directory;

    public StaticHandler(String directory) {
        this.directory = directory == null || directory.isBlank() ? null : Path.of(directory);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String resource = path.endsWith("/") ? path + INDEX : path;
        resource = resource.startsWith("/") ? resource.substring(1) : resource;

        // ! never let a request escape the served directory
        if (resource.contains("..")) {
            send(
                    exchange,
                    400,
                    "text/plain; charset=utf-8",
                    "Bad path".getBytes(StandardCharsets.UTF_8));
            return;
        }

        byte[] body = read(resource);
        if (body == null) {
            send(
                    exchange,
                    404,
                    "text/plain; charset=utf-8",
                    ("Not found: " + path).getBytes(StandardCharsets.UTF_8));
            return;
        }

        send(exchange, 200, contentType(resource), body);
    }

    private byte[] read(String resource) throws IOException {
        if (directory != null) {
            Path file = directory.resolve(resource).normalize();
            if (file.startsWith(directory) && Files.isReadable(file) && !Files.isDirectory(file)) {
                return Files.readAllBytes(file);
            }
            return null;
        }

        try (InputStream in =
                StaticHandler.class
                        .getClassLoader()
                        .getResourceAsStream(CLASSPATH_DIR + "/" + resource)) {
            return in == null ? null : in.readAllBytes();
        }
    }

    private static String contentType(String resource) {
        int dot = resource.lastIndexOf('.');
        String extension = dot < 0 ? "" : resource.substring(dot + 1).toLowerCase();
        return CONTENT_TYPES.getOrDefault(extension, "application/octet-stream");
    }

    private static void send(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        // ! the page is edited while the console runs, a cached copy would hide every change
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
