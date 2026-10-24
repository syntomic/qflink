package cn.syntomic.qflink.web.configuration;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The duration syntax of a flink option, {@code 2 s}, {@code 500ms}, {@code 1 min}.
 *
 * <p>The console reads the job property file and has no flink on its classpath to parse it with.
 */
public class Durations {

    private static final Pattern DURATION = Pattern.compile("^(\\d+)\\s*([a-zA-Z]*)$");

    private static final Map<String, Long> UNITS =
            Map.ofEntries(
                    Map.entry("", 1L),
                    Map.entry("ms", 1L),
                    Map.entry("milli", 1L),
                    Map.entry("millis", 1L),
                    Map.entry("millisecond", 1L),
                    Map.entry("milliseconds", 1L),
                    Map.entry("s", 1000L),
                    Map.entry("sec", 1000L),
                    Map.entry("secs", 1000L),
                    Map.entry("second", 1000L),
                    Map.entry("seconds", 1000L),
                    Map.entry("min", 60_000L),
                    Map.entry("mins", 60_000L),
                    Map.entry("minute", 60_000L),
                    Map.entry("minutes", 60_000L),
                    Map.entry("h", 3_600_000L),
                    Map.entry("hour", 3_600_000L),
                    Map.entry("hours", 3_600_000L),
                    Map.entry("d", 86_400_000L),
                    Map.entry("day", 86_400_000L),
                    Map.entry("days", 86_400_000L));

    /**
     * Parse a duration into milliseconds
     *
     * @param value
     * @param defaultValue returned when the value is missing or not a duration
     * @return
     */
    public static long toMillis(String value, long defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        Matcher matcher = DURATION.matcher(value.trim());
        if (!matcher.matches()) {
            return defaultValue;
        }

        Long unit = UNITS.get(matcher.group(2).toLowerCase());
        return unit == null ? defaultValue : Long.parseLong(matcher.group(1)) * unit;
    }

    private Durations() {}
}
