package cn.syntomic.qflink.python.impls;

import static org.apache.flink.python.PythonOptions.PYTHON_EXECUTABLE;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import cn.syntomic.qflink.common.jobs.AbstractJob;
import cn.syntomic.qflink.sql.sdk.jobs.IntegrateJob;

public class FlinkWithPemja extends AbstractJob {

    public static final ConfigOption<String> PYTHON_MODEL =
            ConfigOptions.key("python.model")
                    .stringType()
                    .defaultValue("./qflink-python/src/main/resources/models/demo.joblib")
                    .withDescription("The python model path.");

    public static final ConfigOption<String> PYTHON_SCRIPT =
            ConfigOptions.key("python.script")
                    .stringType()
                    .defaultValue("./qflink-python/src/main/resources/scripts/demo_script.py")
                    .withDescription("The user's python script.");

    @Override
    public void run() throws Exception {
        IntegrateJob integrateJob = IntegrateJob.of(conf, env);
        // use sql to create tables
        integrateJob.run();

        StreamTableEnvironment tableEnv = integrateJob.getTableEnv();

        DataStream<Row> source = tableEnv.toDataStream(tableEnv.from("source"));
        DataStream<Row> transform =
                source.process(new MyProcessFunction())
                        .returns(integrateJob.getRowTypeInfo("sink"));

        tableEnv.fromDataStream(transform).executeInsert("sink");
    }

    private static class MyProcessFunction extends ProcessFunction<Row, Row> {

        private transient PythonInterpreter interpreter = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            Configuration conf =
                    (Configuration)
                            getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

            PythonInterpreterConfig config =
                    PythonInterpreterConfig.newBuilder()
                            .setPythonExec(conf.getString(PYTHON_EXECUTABLE))
                            .build();

            interpreter = new PythonInterpreter(config);

            // exec user script
            interpreter.exec(
                    new String(
                            Files.readAllBytes(Paths.get(conf.getString(PYTHON_SCRIPT))),
                            StandardCharsets.UTF_8));

            // ! invoke has bug, use set instead: https://github.com/alibaba/pemja/issues/26
            interpreter.set("model_path", conf.getString(PYTHON_MODEL));
            interpreter.exec("model=user_open(model_path)");
        }

        @Override
        public void processElement(
                Row value, ProcessFunction<Row, Row>.Context ctx, Collector<Row> out)
                throws Exception {

            interpreter.set("data", value.getField(0));
            interpreter.exec("predict=user_eval(model, data)");
            out.collect(Row.of(interpreter.get("predict", Integer.class)));
        }
    }
}
