package cn.syntomic.qflink.sql.sdk.jobs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import cn.syntomic.qflink.common.utils.EnvUtil;

public class SQLJob {

    protected final Configuration conf;
    protected final StreamExecutionEnvironment env;
    protected final StreamTableEnvironment tableEnv;

    public SQLJob(
            Configuration conf, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        this.conf = conf;
        this.env = env;
        this.tableEnv = tableEnv;
    }

    /**
     * Run the pure sql job
     *
     * @throws Exception
     */
    public void run() throws Exception {
        String[] sqls = SQLFactory.of(conf).readSqls();
        SQLExecutor.of(tableEnv).execute(sqls);
    }

    public Configuration getConf() {
        return conf;
    }

    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public StreamTableEnvironment getTableEnv() {
        return tableEnv;
    }

    /**
     * SQLJob creator
     *
     * @param conf
     * @return
     */
    public static SQLJob of(Configuration conf) {
        StreamExecutionEnvironment env = EnvUtil.setStreamEnv(conf);
        return new SQLJob(conf, env, EnvUtil.setStreamTableEnv(env, conf));
    }

    public static SQLJob of(Configuration conf, StreamExecutionEnvironment env) {
        return new SQLJob(conf, env, EnvUtil.setStreamTableEnv(env, conf));
    }
}
