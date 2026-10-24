package cn.syntomic.qflink.web.channel;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Consumes the kafka topic a rule engine sink writes. */
public class KafkaSubscriber implements Subscriber {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSubscriber.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final String name;
    private final String topic;
    private final String bootstrapServers;
    private final KafkaConsumer<String, String> consumer;

    private volatile boolean running = true;
    private Thread thread;

    public KafkaSubscriber(
            String name, String topic, String bootstrapServers, boolean replay, Properties extra) {
        this.name = name;
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // ! a private group, the console must never commit offsets of the job's own consumers
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "qflink-web-" + name + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, replay ? "earliest" : "latest");
        props.putAll(extra);

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void start(Consumer<String> onRecord) {
        thread = new Thread(() -> poll(onRecord), "qflink-web-consume-" + name);
        thread.setDaemon(true);
        thread.start();
    }

    private void poll(Consumer<String> onRecord) {
        try {
            consumer.subscribe(List.of(topic));
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() != null && !record.value().isBlank()) {
                        onRecord.accept(record.value());
                    }
                }
            }
        } catch (InterruptException e) {
            LOG.debug("The {} consumer was interrupted", name);
        } catch (RuntimeException e) {
            LOG.error("The {} consumer stopped on {}", name, topic, e);
        } finally {
            consumer.close();
        }
    }

    @Override
    public String describe() {
        return String.format("kafka %s@%s", topic, bootstrapServers);
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }
}
