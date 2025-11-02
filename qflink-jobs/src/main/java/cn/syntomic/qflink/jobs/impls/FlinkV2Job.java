package cn.syntomic.qflink.jobs.impls;

import java.util.Arrays;

import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class FlinkV2Job {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream<String> input =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList("1", "2", "3")), "source");

        NonKeyedPartitionStream<Integer> parsed =
                input.process(
                        (OneInputStreamProcessFunction<String, Integer>)
                                (record, output, ctx) -> {
                                    output.collect(Integer.parseInt(record));
                                });

        parsed.toSink(DataStreamV2SinkUtils.wrapSink(new PrintSink<>()));

        env.execute("execute");
    }
}
