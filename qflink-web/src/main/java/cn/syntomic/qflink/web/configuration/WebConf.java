package cn.syntomic.qflink.web.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * The configuration of the console, merged from three sources, <b>command line args &gt; web
 * property file &gt; rule job property file</b>.
 *
 * <p>The job property file is the one the rule engine itself reads, so the console always talks to
 * the channels the job is configured with instead of a second copy of them.
 */
public class WebConf {

    private static final String DEFAULT_ENV = "develop";
    private static final String PROPS_FORMAT_DIR = "configs-%s";
    private static final String WEB_PROPS_FILE = "web.properties";

    private final Map<String, String> props = new TreeMap<>();

    private WebConf() {}

    /**
     * Build the configuration of a console run
     *
     * @param args unix style command line args, {@code --key value}
     * @return
     */
    public static WebConf load(String[] args) {
        Map<String, String> cli = parseArgs(args);
        String env = cli.getOrDefault(WebOptions.ENV, DEFAULT_ENV);

        WebConf conf = new WebConf();
        conf.props.putAll(
                readResource(String.format(PROPS_FORMAT_DIR, env) + "/" + WEB_PROPS_FILE));
        conf.props.putAll(cli);

        // the rule job property file is read last but only fills the keys nobody set before
        String jobProps = conf.get(WebOptions.JOB_PROPERTIES);
        if (jobProps != null) {
            Map<String, String> job = readFile(Path.of(jobProps));
            job.forEach(conf.props::putIfAbsent);
        }

        return conf;
    }

    /** Build a configuration from an explicit map, used by the tests. */
    public static WebConf of(Map<String, String> props) {
        WebConf conf = new WebConf();
        conf.props.putAll(props);
        return conf;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public String get(String key) {
        return props.get(key);
    }

    public String get(String key, String defaultValue) {
        String value = props.get(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    /**
     * Read an option under a prefix and fall back to the option without it, the same way the job
     * configuration does, so {@code alert.sink} falls back to {@code sink}.
     *
     * @param prefix e.g. log, rule, alert, error
     * @param key
     * @param defaultValue
     * @return
     */
    public String get(String prefix, String key, String defaultValue) {
        return get(prefix + "." + key, get(key, defaultValue));
    }

    public int getInt(String key, int defaultValue) {
        String value = get(key);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value.trim());
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = get(key);
        return value == null || value.isBlank() ? defaultValue : Boolean.parseBoolean(value.trim());
    }

    /** An ordered copy of every option, exposed to the console overview page. */
    public Map<String, String> toMap() {
        return new LinkedHashMap<>(props);
    }

    // -------------------------------------------------------------------------
    // Sources
    // -------------------------------------------------------------------------

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parsed = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException(
                        "Unix style args are expected, --key value, but got " + arg);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing the value of " + arg);
            }
            parsed.put(arg.substring(2), args[++i]);
        }
        return parsed;
    }

    private static Map<String, String> readResource(String resource) {
        try (InputStream in = WebConf.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                return Map.of();
            }
            return read(new InputStreamReader(in, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read the resource " + resource, e);
        }
    }

    private static Map<String, String> readFile(Path path) {
        if (!Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot read the rule job properties %s, set %s to the file the job"
                                    + " reads",
                            path.toAbsolutePath(), WebOptions.JOB_PROPERTIES));
        }

        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return read(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + path, e);
        }
    }

    private static Map<String, String> read(Reader reader) throws IOException {
        Properties properties = new Properties();
        properties.load(reader);

        Map<String, String> values = new LinkedHashMap<>();
        properties.forEach((key, value) -> values.put(String.valueOf(key), String.valueOf(value)));
        return values;
    }
}
