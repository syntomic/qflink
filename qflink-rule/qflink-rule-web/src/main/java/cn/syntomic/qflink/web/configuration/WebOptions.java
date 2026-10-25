package cn.syntomic.qflink.web.configuration;

/** The options of the console itself, everything else is read from the rule job properties. */
public class WebOptions {

    /** {@code develop}, {@code test} or {@code produce}, decides the property directory. */
    public static final String ENV = "env";

    /** The property file the rule engine job reads, the console derives its channels from it. */
    public static final String JOB_PROPERTIES = "web.job-properties";

    public static final String PORT = "web.port";

    /** The directory the qfile source reads from, only used when a channel is a file. */
    public static final String SOURCES_DIR = "web.sources-dir";

    /** The directory the qfile sink writes to, only used when a channel is a file. */
    public static final String SINKS_DIR = "web.sinks-dir";

    /** Where the console keeps the rules it manages. */
    public static final String DATA_DIR = "web.data-dir";

    /**
     * Read a channel from its beginning, the alert table is then rebuilt from the channel on start.
     */
    public static final String REPLAY = "web.replay";

    /** The SQL2API definitions of the data service, {@code /api/data/...}. */
    public static final String DATA_API_DIR = "web.data-api-dir";

    /**
     * Serve the page from the source directory instead of the classpath, no rebuild while editing.
     */
    public static final String STATIC_DIR = "web.static-dir";

    /** How many alerts, errors and logs are kept in memory for a page which just opened. */
    public static final String HISTORY_SIZE = "web.history-size";

    /** The log line template of the generator, {@code ${...}} placeholders are expanded. */
    public static final String GENERATOR_TEMPLATE = "web.generator.template";

    // ---- the job options the console reads, the keys are the job's own ----

    public static final String JOB_ID = "job.id";
    public static final String ETL_OUTPUT_SCHEMA = "etl.output-schema";
    public static final String LOG_FORMAT = "log.format";
    public static final String LOG_FIELD_TIME = "log.field-time";

    public static final String SOURCE = "source";
    public static final String SINK = "sink";
    public static final String TOPIC = "topic";
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String LOG = "log";
    public static final String RULE = "rule";
    public static final String ALERT = "alert";
    public static final String ERROR = "error";

    private WebOptions() {}
}
