package cn.syntomic.qflink.common.jobs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.common.configuration.ConfigurationFactory;
import cn.syntomic.qflink.common.utils.EnvUtil;

/** The flink job unified entrance */
public class JobEntrance {
    private static final Logger LOG = LoggerFactory.getLogger(JobEntrance.class);

    public static final ConfigOption<String> CLASS_NAME =
            ConfigOptions.key("job.class-name")
                    .stringType()
                    .defaultValue("WordCountJob")
                    .withDescription("The Class Name of Job");

    public static void run(String[] args, String defaultJob) {
        try {
            // parse args to job configuration
            Configuration conf = ConfigurationFactory.of(args).argsWithDefault();

            run(conf, defaultJob);
        } catch (Exception e) {
            LOG.error("Job Run Error", e);
        }
    }

    @VisibleForTesting
    public static void run(Configuration conf, String defaultJob) {
        try {
            // construct job instance
            String className = conf.get(CLASS_NAME, defaultJob);
            AbstractJob job =
                    (AbstractJob) Class.forName(className).getDeclaredConstructor().newInstance();
            job.setConf(conf);
            job.setEnv(EnvUtil.setStreamEnv(conf));

            // execute job
            job.open();
            job.run();
        } catch (Exception e) {
            LOG.error("Job Run Error", e);
        }
    }

    private JobEntrance() {}
}
