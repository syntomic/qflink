package cn.syntomic.qflink.common.connectors.helper.kafka;

import static cn.syntomic.qflink.common.connectors.ConnectorOptions.DEFAULT_SINK;
import static cn.syntomic.qflink.common.connectors.ConnectorOptions.DEFAULT_SOURCE;
import static cn.syntomic.qflink.common.connectors.ConnectorOptions.SINK;
import static cn.syntomic.qflink.common.connectors.ConnectorOptions.SOURCE;
import static cn.syntomic.qflink.common.connectors.helper.kafka.KafkaOptions.KAFKA_PROPERTIES;
import static cn.syntomic.qflink.common.connectors.helper.kafka.KafkaOptions.SASL_PASSWORD;
import static cn.syntomic.qflink.common.connectors.helper.kafka.KafkaOptions.SASL_USERNAME;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TRANSACTIONAL_ID_PREFIX;

import java.time.Duration;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import cn.syntomic.qflink.common.configuration.QConfiguration;
import cn.syntomic.qflink.common.connectors.helper.ConnectorHelper;
import cn.syntomic.qflink.common.connectors.source.qfile.QFileSource;

/** Kafka best practice */
public class KafkaHelper extends ConnectorHelper {

    protected KafkaHelper(QConfiguration conf) {
        super(conf);
    }

    /**
     * FLIP-27 Unified Source
     *
     * @param <T>
     * @param deserializationSchema
     * @param sourceName
     * @return
     */
    public <T> Source<T, ?, ?> createSource(
            DeserializationSchema<T> deserializationSchema, String sourceName) {

        return KafkaSource.<T>builder()
                .setBootstrapServers(conf.get(sourceName, PROPS_BOOTSTRAP_SERVERS))
                .setGroupId(conf.get(sourceName, PROPS_GROUP_ID))
                .setTopics(conf.get(sourceName, TOPIC))
                // ! value only
                .setValueOnlyDeserializer(deserializationSchema)
                .setStartingOffsets(setStartingOffsets(sourceName))
                // dynamic Partition Discovery
                .setProperty(
                        "partition.discovery.interval.ms",
                        String.valueOf(
                                conf.get(
                                                sourceName,
                                                SCAN_TOPIC_PARTITION_DISCOVERY,
                                                Duration.ofMinutes(5))
                                        .toMillis()))
                .setProperties(initProperties(true, sourceName))
                .build();
    }

    /**
     * FLIP-143 Unified Sink
     *
     * @param <T>
     * @param serializationSchema
     * @param topicSelector
     * @param sinkName
     * @return
     */
    public <T> Sink<T> createSink(
            SerializationSchema<T> serializationSchema,
            TopicSelector<T> topicSelector,
            String sinkName) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(conf.get(sinkName, PROPS_BOOTSTRAP_SERVERS))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopicSelector(topicSelector)
                                .setValueSerializationSchema(serializationSchema)
                                .build())
                .setDeliveryGuarantee(conf.get(sinkName, DELIVERY_GUARANTEE))
                // ! not effect in at-least-once mode
                .setTransactionalIdPrefix(conf.get(sinkName, TRANSACTIONAL_ID_PREFIX))
                .setKafkaProducerConfig(initProperties(false, sinkName))
                .build();
    }

    /**
     * Set kafka consume start offsets
     *
     * @param sourceName name of kafka source
     * @return
     */
    private OffsetsInitializer setStartingOffsets(String sourceName) {
        switch (conf.get(sourceName, SCAN_STARTUP_MODE)) {
            case LATEST_OFFSET:
                return OffsetsInitializer.latest();
            case EARLIEST_OFFSET:
                return OffsetsInitializer.earliest();
            case GROUP_OFFSETS:
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
            case TIMESTAMP:
                return OffsetsInitializer.timestamp(
                        conf.get(sourceName, SCAN_STARTUP_TIMESTAMP_MILLIS));
            default:
                throw new UnsupportedOperationException(
                        "Not support scam mode: " + conf.get(sourceName, SCAN_STARTUP_MODE));
        }
    }

    /**
     * Initialize kafka properties
     *
     * @param isSource
     * @param name
     * @return
     */
    private Properties initProperties(boolean isSource, String name) {

        Properties kafkaProps = new Properties();

        // security
        if (conf.get(name, SASL_USERNAME) != null) {
            // ! Security now only support SASL_PLAINTEXT protocol +  PLAIN mechanism
            kafkaProps.setProperty("security.protocol", "SASL_PLAINTEXT");
            kafkaProps.setProperty("sasl.mechanism", "PLAIN");
            kafkaProps.setProperty("sasl.kerberos.service.name", "kafka");
            kafkaProps.setProperty(
                    "sasl.jaas.config",
                    String.format(
                            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                            conf.get(name, SASL_USERNAME), conf.get(name, SASL_PASSWORD)));
        }

        // exactly once
        if (conf.get(name, DELIVERY_GUARANTEE) == DeliveryGuarantee.EXACTLY_ONCE) {
            if (isSource) {
                kafkaProps.setProperty("isolation.level", "read_committed");
            } else {
                // ! Tweak Kafka transaction timeout >> maximum checkpoint duration + maximum restart duration
                kafkaProps.setProperty("transaction.timeout.ms", "600000");
            }
        }

        kafkaProps.putAll(conf.get(name, KAFKA_PROPERTIES));
        return kafkaProps;
    }

    /**
     * Create kafka DataStream source
     *
     * <p><b>Source name must be unique in one job</b>
     *
     * @param <T>
     * @param env
     * @param deserializationSchema
     * @param watermarkStrategy
     * @param sourceName
     * @return
     */
    public <T> SingleOutputStreamOperator<T> createDataStreamSource(
            StreamExecutionEnvironment env,
            DeserializationSchema<T> deserializationSchema,
            WatermarkStrategy<T> watermarkStrategy,
            String sourceName) {
        if (DEFAULT_SOURCE.equalsIgnoreCase(conf.get(sourceName, SOURCE))) {
            return env.fromSource(
                            QFileSource.<T>of(conf, deserializationSchema, sourceName),
                            watermarkStrategy,
                            sourceName)
                    .uid(sourceName);
        }

        Source<T, ?, ?> source = createSource(deserializationSchema, sourceName);
        return createDataStreamSource(env, source, watermarkStrategy, sourceName);
    }

    /**
     * Create kafka DataStream source
     *
     * <p><b>Source name must be unique in one job</b>
     *
     * @param <T>
     * @param env
     * @param deserializationSchema
     * @param sourceName
     * @return
     */
    public <T> SingleOutputStreamOperator<T> createDataStreamSource(
            StreamExecutionEnvironment env,
            DeserializationSchema<T> deserializationSchema,
            String sourceName) {
        return createDataStreamSource(
                env, deserializationSchema, WatermarkStrategy.noWatermarks(), sourceName);
    }

    /**
     * Create kafka DataStream sink
     *
     * <p><b>Sink name must be unique in one job</b>
     *
     * @param <T>
     * @param dataStream
     * @param serializationSchema
     * @param sinkName
     * @return
     */
    public <T> DataStreamSink<T> createDataStreamSink(
            DataStream<T> dataStream, SerializationSchema<T> serializationSchema, String sinkName) {
        if (DEFAULT_SINK.equalsIgnoreCase(conf.get(sinkName, SINK))) {
            return createDataStreamSink(dataStream, new PrintSink<>(), sinkName);
        }
        return createDataStreamSink(
                dataStream,
                serializationSchema,
                new SingleTopicSelector<T>(conf.get(sinkName, TOPIC).get(0)),
                sinkName);
    }

    /**
     * Create DataStream Sink
     *
     * @param <T>
     * @param dataStream
     * @param serializationSchema
     * @param topicSelector
     * @param sinkName
     * @return
     */
    public <T> DataStreamSink<T> createDataStreamSink(
            DataStream<T> dataStream,
            SerializationSchema<T> serializationSchema,
            TopicSelector<T> topicSelector,
            String sinkName) {
        if (DEFAULT_SINK.equalsIgnoreCase(conf.get(sinkName, SINK))) {
            return createDataStreamSink(dataStream, new PrintSink<>(), sinkName);
        }
        Preconditions.checkState(
                conf.get(sinkName, TOPIC).size() == 1, "Topic list is not supported for sinks.");
        Sink<T> sink = createSink(serializationSchema, topicSelector, sinkName);
        return createDataStreamSink(dataStream, sink, sinkName);
    }

    /**
     * Create Kafka Helper
     *
     * @param conf
     * @return
     */
    public static KafkaHelper of(Configuration conf) {
        return new KafkaHelper(new QConfiguration(conf));
    }
}
