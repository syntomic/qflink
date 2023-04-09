package cn.syntomic.qflink.rule;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ALERT_TYPE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ERROR_TAG;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ERROR_TYPE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.KEY;
import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RULE_ID;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_ENABLE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.AGG_PARALLELISM;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.ETL_OUTPUT;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.ETL_OUTPUT_SCHEMA;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.JOB_ID;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.LOG_FIELD_TIME;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.LOG_FORMAT;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.LOG_SCHEMA;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.WATERMARK_TIMING;

import java.util.Optional;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.ObjectArrays;

import cn.syntomic.qflink.common.codec.deser.JsonPojoDeserializationSchema;
import cn.syntomic.qflink.common.connectors.helper.kafka.KafkaHelper;
import cn.syntomic.qflink.common.jobs.AbstractJob;
import cn.syntomic.qflink.common.utils.WatermarkUtil;
import cn.syntomic.qflink.rule.codec.deser.LogRowDeserializationSchema;
import cn.syntomic.qflink.rule.codec.ser.DynamicTopicSelector;
import cn.syntomic.qflink.rule.codec.ser.JsonRowSerializationSchema;
import cn.syntomic.qflink.rule.codec.ser.LogJsonRowSerializationSchema;
import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ETLOutput;
import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.WatermarkTiming;
import cn.syntomic.qflink.rule.datastream.DynamicWindowedStream;
import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.AvgAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.CountAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.CountDistinctAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.MaxAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.MinAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.SumAccumulator;
import cn.syntomic.qflink.rule.functions.eventtime.LogRowTimestampAssigner;

public class RuleEngineJob extends AbstractJob {

    private KafkaHelper kafkaHelper;

    @Override
    public void open() {
        // connector helper
        kafkaHelper = KafkaHelper.of(conf);

        // register subtype for generic type
        env.registerType(SumAccumulator.class);
        env.registerType(MaxAccumulator.class);
        env.registerType(MinAccumulator.class);
        env.registerType(AvgAccumulator.class);
        env.registerType(CountAccumulator.class);
        env.registerType(CountDistinctAccumulator.class);
    }

    @Override
    public void run() throws Exception {
        // log source + broadcast rule source
        DataStream<Row> logStream = createLogSource();
        DataStream<Rule> ruleStream = createRuleSource();

        // etl by rule
        SingleOutputStreamOperator<Row> dynamicEtlStream = dynamicEtl(logStream, ruleStream);

        // aggregate by rule
        Optional<SingleOutputStreamOperator<Row>> dynamicAggStream =
                dynamicAgg(dynamicEtlStream, ruleStream);

        // side output errors
        DataStream<Row> errorStreams = getErrorStream(dynamicEtlStream, dynamicAggStream);

        createSink(errorStreams, dynamicEtlStream, dynamicAggStream);

        env.execute(String.format("Rule Engine: %s Job", conf.get(JOB_ID)));
    }

    //  ----------------------------------------------------

    private DataStream<Row> createLogSource() {
        DeserializationSchema<Row> deserializationSchema =
                new LogRowDeserializationSchema(conf.get(LOG_FORMAT), conf.get(LOG_SCHEMA));

        if (conf.get(WATERMARK_TIMING) == WatermarkTiming.SOURCE) {
            return kafkaHelper.createDataStreamSource(
                    env,
                    deserializationSchema,
                    WatermarkUtil.setBoundedOutOfOrderness(
                            new LogRowTimestampAssigner(conf.get(LOG_FIELD_TIME)), conf, "log"),
                    "log");
        } else {
            return kafkaHelper.createDataStreamSource(env, deserializationSchema, "log");
        }
    }

    private DataStream<Rule> createRuleSource() {
        return kafkaHelper
                .createDataStreamSource(
                        env,
                        new JsonPojoDeserializationSchema<Rule>(Rule.class, true),
                        WatermarkUtil.setMax(),
                        "rule")
                .setParallelism(1);
    }

    private SingleOutputStreamOperator<Row> dynamicEtl(
            DataStream<Row> logStream, DataStream<Rule> ruleStream) {
        return DynamicWindowedStream.of(env, logStream, ruleStream).process(getEtlOutputType());
    }

    private Optional<SingleOutputStreamOperator<Row>> dynamicAgg(
            SingleOutputStreamOperator<Row> dynamicEtlStream, DataStream<Rule> ruleStream) {
        if (conf.getBoolean(AGG_ENABLE)) {
            Preconditions.checkNotNull(
                    conf.get(LOG_FIELD_TIME), "Agg enable must specific the time field");
            Preconditions.checkArgument(
                    conf.get(ETL_OUTPUT) != ETLOutput.GENERIC,
                    "Agg enable must specific the input type info");

            if (conf.get(WATERMARK_TIMING) == WatermarkTiming.AFTER_ETL) {
                dynamicEtlStream =
                        dynamicEtlStream.assignTimestampsAndWatermarks(
                                WatermarkUtil.setBoundedOutOfOrderness(
                                        new LogRowTimestampAssigner(conf.get(LOG_FIELD_TIME)),
                                        conf,
                                        "etl"));
            }
            return Optional.of(
                    DynamicWindowedStream.of(
                                    env,
                                    dynamicEtlStream.keyBy(
                                            new KeySelector<Row, Tuple2<Integer, String>>() {
                                                @Override
                                                public Tuple2<Integer, String> getKey(Row value)
                                                        throws Exception {
                                                    // ! keyBy operation before flink serialization,
                                                    // row may not in named-mode
                                                    return value.getFieldNames(false) == null
                                                            ? Tuple2.of(
                                                                    value.getFieldAs(0),
                                                                    value.getFieldAs(1))
                                                            : Tuple2.of(
                                                                    value.getFieldAs(RULE_ID),
                                                                    value.getFieldAs(KEY));
                                                }
                                            },
                                            Types.TUPLE(Types.INT, Types.STRING)),
                                    ruleStream)
                            .process(ALERT_TYPE)
                            .setParallelism(
                                    conf.getInteger(AGG_PARALLELISM, env.getParallelism())));
        } else {
            return Optional.empty();
        }
    }

    private DataStream<Row> getErrorStream(
            SingleOutputStreamOperator<Row> dynamicEtlStream,
            Optional<SingleOutputStreamOperator<Row>> dynamicAggStream) {
        DataStream<Row> dynamicEtlError = dynamicEtlStream.getSideOutput(ERROR_TAG);

        if (dynamicAggStream.isPresent()) {
            return dynamicEtlError.union(dynamicAggStream.get().getSideOutput(ERROR_TAG));
        } else {
            return dynamicEtlError;
        }
    }

    private void createSink(
            DataStream<Row> errorStreams,
            SingleOutputStreamOperator<Row> dynamicEtlStream,
            Optional<SingleOutputStreamOperator<Row>> dynamicAggStream) {

        if (dynamicAggStream.isPresent()) {
            kafkaHelper.createDataStreamSink(
                    dynamicAggStream.get(),
                    JsonRowSerializationSchema.builder().withTypeInfo(ALERT_TYPE).build(),
                    "alert");
        } else {
            // TODO support filesystem
            kafkaHelper.createDataStreamSink(
                    dynamicEtlStream,
                    new LogJsonRowSerializationSchema(getEtlOutputType()),
                    new DynamicTopicSelector(),
                    "etl");
        }

        kafkaHelper.createDataStreamSink(
                errorStreams,
                JsonRowSerializationSchema.builder().withTypeInfo(ERROR_TYPE).build(),
                "error");
    }

    private TypeInformation<Row> getEtlOutputType() {
        switch (conf.get(ETL_OUTPUT)) {
            case INHERIT:
                RowTypeInfo logSchema =
                        (RowTypeInfo)
                                AvroSchemaConverter.<Row>convertToTypeInfo(
                                        conf.getString(LOG_SCHEMA));
                if (conf.getBoolean(AGG_ENABLE)) {
                    return Types.ROW_NAMED(
                            ObjectArrays.concat(
                                    new String[] {RULE_ID, KEY},
                                    logSchema.getFieldNames(),
                                    String.class),
                            ObjectArrays.concat(
                                    new TypeInformation<?>[] {Types.INT, Types.STRING},
                                    logSchema.getFieldTypes(),
                                    TypeInformation.class));
                } else {
                    return logSchema;
                }
            case SPECIFIC:
                // if need aggregate, must specific rule_id and key in position 0 and 1
                return AvroSchemaConverter.<Row>convertToTypeInfo(
                        conf.getString(ETL_OUTPUT_SCHEMA));
            case GENERIC:
            default:
                return Types.GENERIC(Row.class);
        }
    }
}
