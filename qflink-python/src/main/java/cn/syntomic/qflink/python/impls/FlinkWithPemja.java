package cn.syntomic.qflink.python.impls;

import static org.apache.flink.python.PythonOptions.PYTHON_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_PATH;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

        // ! executeInsert is async, without await the job is never run in a local environment
        tableEnv.fromDataStream(transform).executeInsert("sink").await();
    }

    private static class MyProcessFunction extends ProcessFunction<Row, Row> {

        private static final Logger LOG = LoggerFactory.getLogger(MyProcessFunction.class);

        private transient Configuration conf;
        private transient PythonInterpreter interpreter;

        /** The thread which created the interpreter, the only one allowed to use it. */
        private transient Thread owner;

        @Override
        public void open(OpenContext parameters) throws Exception {
            // ! getGlobalJobParameters returns a map, it cannot be cast to Configuration
            this.conf = Configuration.fromMap(getRuntimeContext().getGlobalJobParameters());
        }

        @Override
        public void processElement(
                Row value, ProcessFunction<Row, Row>.Context ctx, Collector<Row> out)
                throws Exception {

            PythonInterpreter python = interpreter();
            python.set("data", value.getField(0));
            python.exec("predict=user_eval(model, data)");
            out.collect(Row.of(python.get("predict", Integer.class)));
        }

        /**
         * Create the interpreter on first use.
         *
         * <p><b>A pemja interpreter is bound to the thread which creates it, using it from another
         * thread deadlocks in the python GIL. open() and processElement do not always run on the
         * same thread(a chained legacy source runs in its own thread), so the interpreter must be
         * created by the thread which calls it.</b>
         */
        private PythonInterpreter interpreter() throws IOException {
            if (interpreter != null) {
                return interpreter;
            }

            PythonInterpreterConfig.PythonInterpreterConfigBuilder builder =
                    PythonInterpreterConfig.newBuilder().setPythonExec(conf.get(PYTHON_EXECUTABLE));

            // ! the embedded interpreter does not see the site-packages of a virtual environment,
            // ! python.pythonpath must point to them, otherwise even `import pemja` fails
            String pythonPath = conf.get(PYTHON_PATH);
            if (pythonPath != null) {
                builder.addPythonPaths(pythonPath.split(File.pathSeparator));
            }

            PythonInterpreter python = new PythonInterpreter(builder.build());

            // exec user script
            python.exec(
                    new String(
                            Files.readAllBytes(Paths.get(conf.get(PYTHON_SCRIPT))),
                            StandardCharsets.UTF_8));

            // ! invoke has bug, use set instead: https://github.com/alibaba/pemja/issues/26
            python.set("model_path", conf.get(PYTHON_MODEL));
            python.exec("model=user_open(model_path)");

            this.owner = Thread.currentThread();
            this.interpreter = python;
            return python;
        }

        @Override
        public void close() throws Exception {
            // ! the interpreter holds a python environment, it leaks on every restart otherwise
            if (interpreter != null) {
                if (Thread.currentThread() == owner) {
                    interpreter.close();
                } else {
                    // closing it here would deadlock the same way using it does
                    LOG.warn(
                            "The python interpreter of thread {} cannot be closed by thread {}",
                            owner,
                            Thread.currentThread());
                }
                interpreter = null;
            }
            super.close();
        }
    }
}
