package cn.syntomic.qflink.rule.datastream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SingleOutputStreamOperatorAdapter<T> extends SingleOutputStreamOperator<T> {

    public SingleOutputStreamOperatorAdapter(
            StreamExecutionEnvironment environment, Transformation<T> transformation) {
        super(environment, transformation);
    }
}
