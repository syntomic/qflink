package cn.syntomic.qflink.web.channel;

import static cn.syntomic.qflink.web.configuration.WebOptions.BOOTSTRAP_SERVERS;
import static cn.syntomic.qflink.web.configuration.WebOptions.SINK;
import static cn.syntomic.qflink.web.configuration.WebOptions.SINKS_DIR;
import static cn.syntomic.qflink.web.configuration.WebOptions.SOURCE;
import static cn.syntomic.qflink.web.configuration.WebOptions.SOURCES_DIR;
import static cn.syntomic.qflink.web.configuration.WebOptions.TOPIC;

import java.nio.file.Path;
import java.util.Properties;

import cn.syntomic.qflink.web.configuration.WebConf;

/**
 * Builds the channels of the console out of the rule job configuration.
 *
 * <p>A source the job reads with {@code qfile} is a file the console appends to, a source it reads
 * with {@code kafka} is a topic the console produces to, and the other way round for the sinks. The
 * console never decides the transport on its own, it only mirrors what the job was told to use.
 */
public class Channels {

    public static final String QFILE = "qfile";
    public static final String PRINT = "print";

    private final WebConf conf;

    public Channels(WebConf conf) {
        this.conf = conf;
    }

    /**
     * The channel a job source reads, e.g. rule or log
     *
     * @param name
     * @return
     */
    public Publisher publisher(String name) {
        String type = conf.get(name, SOURCE, QFILE);
        String topic = topic(name);

        if (QFILE.equalsIgnoreCase(type)) {
            return new FilePublisher(Path.of(conf.get(SOURCES_DIR), topic));
        }
        return new KafkaPublisher(topic, servers(name), kafkaProperties(name));
    }

    /**
     * The channel a job sink writes, e.g. alert or error
     *
     * @param name
     * @return null if the job only prints it, there is nothing to read then
     */
    public Subscriber subscriber(String name) {
        String type = conf.get(name, SINK, PRINT);
        if (PRINT.equalsIgnoreCase(type)) {
            return null;
        }

        String topic = topic(name);
        boolean replay = conf.getBoolean("web.replay", true);

        if (QFILE.equalsIgnoreCase(type)) {
            return new FileTailSubscriber(name, Path.of(conf.get(SINKS_DIR), topic), replay);
        }
        return new KafkaSubscriber(name, topic, servers(name), replay, kafkaProperties(name));
    }

    private String topic(String name) {
        String topic = conf.get(name, TOPIC, null);
        if (topic == null || topic.isBlank()) {
            throw new IllegalStateException(
                    String.format("The %s channel has no %s.%s", name, name, TOPIC));
        }
        return topic;
    }

    private String servers(String name) {
        String servers = conf.get(name, BOOTSTRAP_SERVERS, null);
        if (servers == null || servers.isBlank()) {
            throw new IllegalStateException(
                    String.format(
                            "The %s channel is kafka but has no %s.%s",
                            name, name, BOOTSTRAP_SERVERS));
        }
        return servers;
    }

    /** Every {@code <name>.properties.*} option is passed to the kafka client as is. */
    private Properties kafkaProperties(String name) {
        Properties props = new Properties();
        String prefix = name + ".properties.";

        conf.toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(prefix)) {
                                props.put(key.substring(prefix.length()), value);
                            }
                        });
        return props;
    }
}
