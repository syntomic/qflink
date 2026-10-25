package cn.syntomic.qflink.web.channel;

/** Writes one record into the channel a rule engine source reads, a file or a kafka topic. */
public interface Publisher extends AutoCloseable {

    /**
     * Append one record to the channel
     *
     * @param record a single line, without the line separator
     */
    void publish(String record);

    /** How the channel is configured, shown on the overview page. */
    String describe();

    @Override
    void close();
}
