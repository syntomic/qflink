package cn.syntomic.qflink.rule;

import static cn.syntomic.qflink.common.configuration.PropsOptions.ENV;
import static cn.syntomic.qflink.common.connectors.ConnectorOptions.SINK;
import static cn.syntomic.qflink.common.connectors.ConnectorOptions.SOURCE;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.INTERVAL;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.PATH;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.PAUSE;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.SCAN_TYPE;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.ETL_OUTPUT;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.ETL_OUTPUT_SCHEMA;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.JOB_ID;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.LOG_FIELD_TIME;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.LOG_FORMAT;
import static cn.syntomic.qflink.rule.configuration.RuleEngineOptions.WATERMARK_TIMING;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;

import cn.syntomic.qflink.common.configuration.PropsOptions.Env;
import cn.syntomic.qflink.common.jobs.JobEntrance;
import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ETLOutput;
import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.LogFormat;
import cn.syntomic.qflink.rule.configuration.RuleEngineConstants.WatermarkTiming;

public class RuleEngineJobTest {

    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;

    static final Logger log = org.slf4j.LoggerFactory.getLogger(RuleEngineJobTest.class);

    Configuration conf;

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .setConfiguration(new Configuration())
                            .build());

    @BeforeEach
    void setUpStreams() {
        // ! redirect to std out
        System.setOut(new PrintStream(outContent));

        conf = new Configuration();
        conf.set(ENV, Env.TEST);
        conf.setInteger(DEFAULT_PARALLELISM, 1);
        conf.setString(JOB_ID, "test");
        conf.set(INTERVAL, Duration.ofMillis(0));
        conf.setString(SOURCE, "qfile");
        conf.setString(SCAN_TYPE, "once");
        conf.setString(SINK, "print");
        // ! rule arrive first
        conf.setString("log." + PAUSE.key(), "1 s");
    }

    @AfterEach
    void restoreStreams() {
        System.setOut(originalOut);
    }

    @Test
    void testEnd2EndJob() throws Exception {
        String logs =
                "2023-04-09 15:40:05 stdout {\"key1\": 1,\"key2\":\"val1\"}\n"
                        + "2023-04-09 15:41:05 stdout {\"key1\": 2,\"key2\":\"val2\"}\n"
                        + "2023-04-09 15:42:05 stdout {\"key1\": 3,\"key2\":\"val3\"}\n"
                        + "2023-04-09 15:43:05 stdout {\"key1\": 4,\"key2\":\"val4\"}\n"
                        + "2023-04-09 15:44:05 stdout {\"key1\": 5,\"key2\":\"val5\"}";

        String rules =
                "{\"rule_id\":100,\"rule_state\":\"ACTIVE\",\"clear_state\":false,\"job_id\":\"test\",\"filter\":{\"expr\":\"string.contains(raw, 'stdout')\",\"params\":[\"raw\"]},\"flat_map\":{\"pattern\":\"(\\\\d{4}-\\\\d{2}-\\\\d{2} \\\\d{2}:\\\\d{2}:\\\\d{2}) (.*?) (.*)\",\"normal_fields\":[{\"name\":\"time\",\"type\":\"STRING\",\"default\":null},{\"name\":\"key_word\",\"type\":\"STRING\",\"default\":null},{\"name\":\"\",\"type\":\"JSON\",\"json_paths\":[{\"name\":\"key1\",\"expr\":\"$.key1\",\"type\":\"INT\",\"default\":0},{\"name\":\"key2\",\"expr\":\"$.key2\",\"type\":\"STRING\",\"default\":null}],\"default\":null}],\"mapping_fields\":[{\"name\":\"is_odd\",\"type\":\"BOOLEAN\",\"expr\":\"key1 % 2 == 1\",\"default\":null}]},\"keys\":[\"is_odd\"],\"window\":{\"type\":\"TUMBLE\",\"trigger\":\"SINGLE\",\"offset\":0,\"size\":86400000,\"step\":0},\"aggregates\":[{\"name\":\"val_cnt\",\"inputs\":[\"key2\"],\"method\":\"COUNT_DISTINCT\"}],\"threshold\":{\"expr\":\"val_cnt > 1\",\"params\":[\"val_cnt\"]},\"extras\":{\"etl.table\":\"test_topic\"}}";

        String logPath = createTempFile(logs);
        String rulePath = createTempFile(rules);
        conf.set(WATERMARK_TIMING, WatermarkTiming.AFTER_ETL);
        conf.set(LOG_FORMAT, LogFormat.RAW);
        conf.set(LOG_FIELD_TIME, "time");
        conf.setString("log." + PATH.key(), logPath);
        conf.setString("rule." + PATH.key(), rulePath);
        conf.set(ETL_OUTPUT, ETLOutput.SPECIFIC);
        conf.setString(
                ETL_OUTPUT_SCHEMA,
                "{\"type\":\"record\",\"name\":\"default\",\"fields\":[{\"name\":\"rule_id\",\"type\":\"int\"},{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"key_word\",\"type\":\"string\"},{\"name\":\"key1\",\"type\":\"int\"},{\"name\":\"key2\",\"type\":\"string\"},{\"name\":\"is_odd\",\"type\":\"boolean\"}]}");

        JobEntrance.run(conf, "cn.syntomic.qflink.rule.RuleEngineJob");

        log.error(outContent.toString());
        assertThat(outContent.toString())
                .contains(
                        "+I[100, 2023-04-09 15:42:05, {is_odd=true}, {val_cnt=2.0}, {rule_id=100, key1=3, key2=val3, is_odd=true, key_word=stdout, time=2023-04-09 15:42:05, key=true}]")
                .contains(
                        "+I[100, 2023-04-09 15:43:05, {is_odd=false}, {val_cnt=2.0}, {rule_id=100, key1=4, key2=val4, is_odd=false, key_word=stdout, time=2023-04-09 15:43:05, key=false}]")
                .contains(
                        "+I[100, 2023-04-09 15:44:05, {is_odd=true}, {val_cnt=3.0}, {rule_id=100, key1=5, key2=val5, is_odd=true, key_word=stdout, time=2023-04-09 15:44:05, key=true}]");
    }

    /** Creates a temporary file with the contents and returns the absolute path. */
    static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("test", ".txt");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toString();
    }
}
