package cn.syntomic.qflink.rule.datastream;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RULE_STATE_DESCRIPTOR;
import static java.util.Objects.requireNonNull;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.types.Row;

import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.operators.GlobalCoWindowAggOperator;
import cn.syntomic.qflink.rule.operators.LocalCoWindowAggOperator;

/** Dynamic window by rule broadcast */
public class DynamicWindowedStream {

    private final StreamExecutionEnvironment environment;
    private final DataStream<Row> dataStream;
    private final BroadcastStream<Rule> ruleStream;

    private DynamicWindowedStream(
            final StreamExecutionEnvironment env,
            final DataStream<Row> dataStream,
            final BroadcastStream<Rule> ruleStream) {

        this.environment = requireNonNull(env);
        this.dataStream = requireNonNull(dataStream);
        this.ruleStream = requireNonNull(ruleStream);
    }

    public StreamExecutionEnvironment getExecutionEnvironment() {
        return environment;
    }

    public DataStream<Row> getDataStream() {
        return dataStream;
    }

    public BroadcastStream<Rule> getRuleStream() {
        return ruleStream;
    }

    public TypeInformation<Row> getDataTypeInfo() {
        return dataStream.getType();
    }

    public TypeInformation<Rule> getRuleTypeInfo() {
        return ruleStream.getType();
    }

    /**
     * Process according to data stream keyed status
     *
     * @param outTypeInfo output type information
     * @return
     */
    public SingleOutputStreamOperator<Row> process(final TypeInformation<Row> outTypeInfo) {
        TwoInputStreamOperator<Rule, Row, Row> operator;
        TwoInputTransformation<Rule, Row, Row> transform;

        String name;
        if (dataStream instanceof KeyedStream) {
            KeyedStream<Row, Tuple2<Integer, String>> keyedStream =
                    // ! unchecked cast
                    (KeyedStream<Row, Tuple2<Integer, String>>) dataStream;
            name = "Co-Dynamic-Agg";
            operator = new GlobalCoWindowAggOperator();
            transform =
                    new TwoInputTransformation<>(
                            ruleStream.getTransformation(),
                            dataStream.getTransformation(),
                            name,
                            operator,
                            outTypeInfo,
                            environment.getParallelism());

            transform.setStateKeySelectors(null, keyedStream.getKeySelector());
            transform.setStateKeyType(keyedStream.getKeyType());
        } else {
            name = "Co-Dynamic-ETL";
            operator = new LocalCoWindowAggOperator();
            transform =
                    new TwoInputTransformation<>(
                            ruleStream.getTransformation(),
                            dataStream.getTransformation(),
                            name,
                            operator,
                            outTypeInfo,
                            environment.getParallelism());
        }

        SingleOutputStreamOperator<Row> returnStream =
                new SingleOutputStreamOperatorAdapter<>(environment, transform).uid(name);

        getExecutionEnvironment().addOperator(transform);
        return returnStream;
    }

    /**
     * Combine data stream with rule stream to generate dynamic window stream
     *
     * @param env
     * @param dataStream
     * @param ruleStream
     * @return
     */
    public static DynamicWindowedStream of(
            final StreamExecutionEnvironment env,
            final DataStream<Row> dataStream,
            final DataStream<Rule> ruleStream) {
        return new DynamicWindowedStream(
                requireNonNull(env),
                requireNonNull(dataStream),
                requireNonNull(ruleStream).broadcast(RULE_STATE_DESCRIPTOR));
    }
}
