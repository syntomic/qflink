package cn.syntomic.qflink.web.channel;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Produces a record to the kafka topic a rule engine source reads. */
public class KafkaPublisher implements Publisher {

    private final String topic;
    private final String bootstrapServers;
    private final Producer<String, String> producer;

    public KafkaPublisher(String topic, String bootstrapServers, Properties extra) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "qflink-web");
        props.putAll(extra);

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void publish(String record) {
        // ! the rule source is value only, the key would be dropped anyway
        producer.send(new ProducerRecord<>(topic, record));
        producer.flush();
    }

    @Override
    public String describe() {
        return String.format("kafka %s@%s", topic, bootstrapServers);
    }

    @Override
    public void close() {
        producer.close();
    }
}
