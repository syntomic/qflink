package cn.syntomic.qflink.web.channel;

import java.util.function.Consumer;

/** Reads the records a rule engine sink writes, a file or a kafka topic. */
public interface Subscriber extends AutoCloseable {

    /**
     * Start to consume in a daemon thread, the callback is invoked once per record
     *
     * @param onRecord
     */
    void start(Consumer<String> onRecord);

    /** How the channel is configured, shown on the overview page. */
    String describe();

    @Override
    void close();
}
