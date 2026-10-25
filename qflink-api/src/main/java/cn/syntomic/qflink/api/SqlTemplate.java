package cn.syntomic.qflink.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A sql with {@code {{$name}}} placeholders, rendered into a prepared statement.
 *
 * <p><b>A request parameter is never concatenated into the sql as it is.</b> What a parameter may
 * become depends on its kind:
 *
 * <ul>
 *   <li>{@link Kind#VALUE}: a {@code ?} bound by the driver, {@code ' OR '1'='1} stays a string
 *   <li>{@link Kind#NUMBER}: a bound {@code ?} as well, checked to be an integer under its maximum
 *   <li>{@link Kind#IDENTIFIER}: a part a driver cannot bind, e.g. ORDER BY, only one of the
 *       allowed values is written into the sql
 * </ul>
 */
public class SqlTemplate {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{\\$(\\w+)}}");

    private final String sql;
    private final Map<String, Param> params;

    public SqlTemplate(String sql, Map<String, Param> params) {
        this.sql = sql;
        this.params = params;
        Matcher matcher = PLACEHOLDER.matcher(sql);
        while (matcher.find()) {
            if (!params.containsKey(matcher.group(1))) {
                throw new IllegalStateException("No parameter declared for " + matcher.group());
            }
        }
    }

    /**
     * Render the template with the request parameters
     *
     * @param request parameter name to its raw value, a missing one takes its default
     * @return the sql with {@code ?} and the values to bind, in order
     * @throws IllegalArgumentException when a parameter is missing or not allowed
     */
    public BoundSql render(Map<String, String> request) {
        StringBuilder rendered = new StringBuilder();
        List<Object> args = new ArrayList<>();
        Matcher matcher = PLACEHOLDER.matcher(sql);
        while (matcher.find()) {
            String name = matcher.group(1);
            Param param = params.get(name);
            String raw = request.getOrDefault(name, param.defaultValue);
            if (raw == null) {
                throw new IllegalArgumentException("Missing parameter " + name);
            }

            String replacement;
            switch (param.kind) {
                case VALUE:
                    args.add(raw);
                    replacement = "?";
                    break;
                case NUMBER:
                    args.add(number(name, raw, param.max));
                    replacement = "?";
                    break;
                case IDENTIFIER:
                    if (!param.allowed.contains(raw)) {
                        throw new IllegalArgumentException(
                                name + " must be one of " + param.allowed + ", got " + raw);
                    }
                    replacement = raw;
                    break;
                default:
                    throw new IllegalStateException("Unknown kind " + param.kind);
            }
            matcher.appendReplacement(rendered, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(rendered);
        return new BoundSql(rendered.toString(), args);
    }

    static long number(String name, String raw, long max) {
        long value;
        try {
            value = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer, got " + raw);
        }
        // ! the cap keeps one request from pulling the whole table
        if (value < 0 || value > max) {
            throw new IllegalArgumentException(name + " must be between 0 and " + max);
        }
        return value;
    }

    /** What a placeholder may become. */
    public enum Kind {
        VALUE,
        NUMBER,
        IDENTIFIER
    }

    /** A declared placeholder. */
    public static class Param {
        public Kind kind = Kind.VALUE;

        @JsonProperty("default")
        public String defaultValue;

        public List<String> allowed = List.of();
        public long max = Long.MAX_VALUE;
    }

    /** A rendered sql and the values bound to its {@code ?}. */
    public static class BoundSql {
        public final String sql;
        public final List<Object> args;

        BoundSql(String sql, List<Object> args) {
            this.sql = sql;
            this.args = args;
        }
    }
}
