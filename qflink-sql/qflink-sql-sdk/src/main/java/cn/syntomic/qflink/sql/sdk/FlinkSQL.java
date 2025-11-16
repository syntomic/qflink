package cn.syntomic.qflink.sql.sdk;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.common.configuration.ConfigurationFactory;
import cn.syntomic.qflink.sql.sdk.jobs.SQLJob;

public class FlinkSQL {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQL.class);

    public static void main(String[] args) {
        try {
            Configuration conf = ConfigurationFactory.of(args).argsWithDefault();
            SQLJob.of(conf).run();
        } catch (Exception e) {
            LOG.error("Flink SQL run error", e);
        }
    }
}
