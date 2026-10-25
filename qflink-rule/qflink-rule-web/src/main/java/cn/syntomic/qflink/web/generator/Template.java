package cn.syntomic.qflink.web.generator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Expands the {@code ${...}} placeholders of a log line, so the generator can be driven from the
 * page without a build.
 *
 * <table>
 *   <tr><td>{@code ${time}}</td><td>now, the format the log source parses</td></tr>
 *   <tr><td>{@code ${time:pattern}}</td><td>now, with an explicit pattern</td></tr>
 *   <tr><td>{@code ${seq}}</td><td>an increasing counter</td></tr>
 *   <tr><td>{@code ${uuid}}</td><td>a random uuid</td></tr>
 *   <tr><td>{@code ${int(1,10)}}</td><td>a random integer, both bounds included</td></tr>
 *   <tr><td>{@code ${double(0,1)}}</td><td>a random double</td></tr>
 *   <tr><td>{@code ${choice(a|b|c)}}</td><td>one of the values</td></tr>
 * </table>
 */
public class Template {

    public static final String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]*)\\}");

    private final AtomicLong sequence = new AtomicLong();

    /**
     * Expand one line
     *
     * @param template
     * @return
     */
    public String expand(String template) {
        if (template == null || template.isBlank()) {
            return "";
        }

        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder expanded = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(
                    expanded, Matcher.quoteReplacement(value(matcher.group(1).trim())));
        }
        matcher.appendTail(expanded);

        return expanded.toString();
    }

    private String value(String placeholder) {
        if (placeholder.equals("time")) {
            return now(DEFAULT_TIME_FORMAT);
        }
        if (placeholder.startsWith("time:")) {
            return now(placeholder.substring("time:".length()));
        }
        if (placeholder.equals("seq")) {
            return String.valueOf(sequence.incrementAndGet());
        }
        if (placeholder.equals("uuid")) {
            return UUID.randomUUID().toString();
        }

        List<String> args = args(placeholder);
        if (placeholder.startsWith("int(")) {
            long min = Long.parseLong(args.get(0).trim());
            long max = Long.parseLong(args.get(1).trim());
            return String.valueOf(ThreadLocalRandom.current().nextLong(min, max + 1));
        }
        if (placeholder.startsWith("double(")) {
            double min = Double.parseDouble(args.get(0).trim());
            double max = Double.parseDouble(args.get(1).trim());
            return String.format("%.4f", ThreadLocalRandom.current().nextDouble(min, max));
        }
        if (placeholder.startsWith("choice(")) {
            return args.get(ThreadLocalRandom.current().nextInt(args.size())).trim();
        }

        throw new IllegalArgumentException("Unknown placeholder ${" + placeholder + "}");
    }

    /** The arguments of {@code name(a,b)} or {@code choice(a|b)}. */
    private static List<String> args(String placeholder) {
        int open = placeholder.indexOf('(');
        int close = placeholder.lastIndexOf(')');
        if (open < 0 || close < open) {
            throw new IllegalArgumentException("Unknown placeholder ${" + placeholder + "}");
        }

        String raw = placeholder.substring(open + 1, close);
        return List.of(raw.split(placeholder.startsWith("choice(") ? "\\|" : ","));
    }

    private static String now(String pattern) {
        try {
            return LocalDateTime.now().format(DateTimeFormatter.ofPattern(pattern));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Not a time pattern: " + pattern, e);
        }
    }
}
