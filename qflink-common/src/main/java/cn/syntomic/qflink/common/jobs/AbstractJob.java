package cn.syntomic.qflink.common.jobs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Abstract Flink Job */
public abstract class AbstractJob {

    /** Flink Job Configuration */
    protected Configuration conf;

    /** Flink Job Execute Environment */
    protected StreamExecutionEnvironment env;

    public void open() {}

    public abstract void run() throws Exception;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public void setEnv(StreamExecutionEnvironment env) {
        this.env = env;
    }
}
