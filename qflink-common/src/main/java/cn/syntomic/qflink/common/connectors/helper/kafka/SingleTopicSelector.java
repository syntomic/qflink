package cn.syntomic.qflink.common.connectors.helper.kafka;

import org.apache.flink.connector.kafka.sink.TopicSelector;

/** Avoid lambda serialize problem */
public class SingleTopicSelector<T> implements TopicSelector<T> {

    private final String topic;

    public SingleTopicSelector(String topic) {
        this.topic = topic;
    }

    @Override
    public String apply(T t) {
        return topic;
    }
}
