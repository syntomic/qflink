package cn.syntomic.qflink.common.connectors.helper;

import static cn.syntomic.qflink.common.connectors.ConnectorOptions.SINK_PARALLELISM;
import static cn.syntomic.qflink.common.connectors.ConnectorOptions.SOURCE_PARALLELISM;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

import cn.syntomic.qflink.common.configuration.QConfiguration;

public class ConnectorHelper {

    protected final QConfiguration conf;

    protected ConnectorHelper(QConfiguration conf) {
        this.conf = conf;
    }

    /**
     * Create DataStream Source
     *
     * @param <T>
     * @param env
     * @param source
     * @param watermarkStrategy
     * @param name
     * @return
     */
    public <T> SingleOutputStreamOperator<T> createDataStreamSource(
            StreamExecutionEnvironment env,
            Source<T, ?, ?> source,
            WatermarkStrategy<T> watermarkStrategy,
            String name) {
        int sourceParallelism = conf.get(name, SOURCE_PARALLELISM, env.getParallelism());

        return env.fromSource(source, watermarkStrategy, name)
                .uid(name)
                .setParallelism(sourceParallelism);
    }

    /**
     * Create DataStream Source
     *
     * @param <T>
     * @param env
     * @param source
     * @param watermarkStrategy
     * @param name
     * @return
     */
    public <T> SingleOutputStreamOperator<T> createDataStreamSource(
            StreamExecutionEnvironment env,
            SourceFunction<T> source,
            WatermarkStrategy<T> watermarkStrategy,
            String name) {
        int sourceParallelism = conf.get(name, SOURCE_PARALLELISM, env.getParallelism());

        return env.addSource(source, name)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .uid(name)
                .setParallelism(sourceParallelism);
    }

    /**
     * Create DataStream Sink
     *
     * @param <T>
     * @param dataStream
     * @param sink
     * @param name
     * @return
     */
    public <T> DataStreamSink<T> createDataStreamSink(
            DataStream<T> dataStream, Sink<T> sink, String name) {
        int sinkParallelism =
                conf.get(
                        name,
                        SINK_PARALLELISM,
                        dataStream.getExecutionEnvironment().getParallelism());

        return dataStream.sinkTo(sink).name(name).uid(name).setParallelism(sinkParallelism);
    }

    /**
     * Create DataStream Sink
     *
     * @param <T>
     * @param dataStream
     * @param sink
     * @param name
     * @return
     */
    public <T> DataStreamSink<T> createDataStreamSink(
            DataStream<T> dataStream, SinkFunction<T> sink, String name) {
        int sinkParallelism =
                conf.get(
                        name,
                        SINK_PARALLELISM,
                        dataStream.getExecutionEnvironment().getParallelism());

        return dataStream.addSink(sink).name(name).uid(name).setParallelism(sinkParallelism);
    }
}
