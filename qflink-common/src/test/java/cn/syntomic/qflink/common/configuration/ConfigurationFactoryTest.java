package cn.syntomic.qflink.common.configuration;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

public class ConfigurationFactoryTest {

    @Test
    void testPriority() throws Exception {
        String jsonStr =
                "{\"env\":\"develop\",\"parallelism.default\":3,\"props.file\":\"demo-job.properties\"}";
        String[] args = new String[] {jsonStr};

        Configuration conf = ConfigurationFactory.of(args).argsWithDefault();
        assertEquals(3, conf.get(DEFAULT_PARALLELISM));
        assertEquals("failure-rate", conf.get(RESTART_STRATEGY));
        assertEquals(5, conf.get(CHECKPOINTING_INTERVAL).toMinutes());
    }
}
