package cn.syntomic.qflink.common.utils;

import static cn.syntomic.qflink.common.configuration.PropsOptions.ENV;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.PipelineOptions.CLASSPATHS;
import static org.apache.flink.configuration.RestOptions.PORT;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.table.api.config.TableConfigOptions.LOCAL_TIME_ZONE;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.ZoneId;
import java.util.List;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FlinkUserCodeClassLoaders.SafetyNetWrapperClassLoader;

import org.apache.flink.shaded.guava30.com.google.common.collect.ObjectArrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.common.configuration.PropsOptions.Env;

public class EnvUtil {

    private static final Logger LOG = LoggerFactory.getLogger(EnvUtil.class);

    /**
     * Set Flink Job's StreamExecutionEnvironment
     *
     * @param conf job configuration
     * @return
     */
    public static StreamExecutionEnvironment setStreamEnv(Configuration conf) {
        StreamExecutionEnvironment env;
        Configuration configuration = new Configuration();

        // dynamic add jars
        List<String> classpaths = conf.get(CLASSPATHS);
        if (classpaths != null) {
            dynamicAddJars(classpaths);
            configuration.set(CLASSPATHS, classpaths);
        }

        if (conf.get(ENV) == Env.DEVELOP) {
            configuration.setInteger(PORT, conf.getInteger(PORT));
            configuration.setString(STATE_BACKEND, "hashmap");
            configuration.setString(SAVEPOINT_DIRECTORY, "file:///tmp/savepoints");
            configuration.setString(CHECKPOINTS_DIRECTORY, "file:///tmp/checkpoints");
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        }

        if (conf.get(RUNTIME_MODE) == RuntimeExecutionMode.BATCH) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            // Batch mode not need cp
            EnvUtil.setCheckPointConfig(env, conf);
        }

        EnvUtil.setRestartConfig(env, conf);

        // register job conf to global configuration
        env.setMaxParallelism(512);
        env.getConfig().setGlobalJobParameters(conf);
        env.setParallelism(conf.getInteger(DEFAULT_PARALLELISM, env.getParallelism()));
        return env;
    }

    /**
     * Create tableEnv with env by job configs
     *
     * @param env
     * @param conf
     * @return
     */
    public static StreamTableEnvironment setStreamTableEnv(
            StreamExecutionEnvironment env, Configuration conf) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String zone = conf.get(LOCAL_TIME_ZONE);
        ZoneId local =
                LOCAL_TIME_ZONE.defaultValue().equals(zone)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zone);
        // since 1.13
        tableEnv.getConfig().setLocalTimeZone(local);
        return tableEnv;
    }

    /**
     * Set env checkpoint configs by job configs
     *
     * @param env
     * @param conf
     * @return
     */
    private static StreamExecutionEnvironment setCheckPointConfig(
            StreamExecutionEnvironment env, Configuration conf) {

        if (conf.get(CHECKPOINTING_INTERVAL) != null) {
            // best practice
            env.enableCheckpointing(conf.get(CHECKPOINTING_INTERVAL).toMillis());
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
            env.getCheckpointConfig().setCheckpointTimeout(300000);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
            env.getCheckpointConfig()
                    .setExternalizedCheckpointCleanup(
                            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }

        return env;
    }

    /**
     * Set env restart configs by job configs
     *
     * @param env
     * @param conf
     * @return
     */
    private static StreamExecutionEnvironment setRestartConfig(
            StreamExecutionEnvironment env, Configuration conf) {

        String restartStrategy = conf.getString(RESTART_STRATEGY, "failure-rate");
        if ("failure-rate".equalsIgnoreCase(restartStrategy)) {
            env.setRestartStrategy(
                    RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));
        } else if ("fixed-delay".equalsIgnoreCase(restartStrategy)) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        } else if ("exponential-delay".equalsIgnoreCase(restartStrategy)) {
            env.setRestartStrategy(
                    RestartStrategies.exponentialDelayRestart(
                            Time.milliseconds(1),
                            Time.milliseconds(1000),
                            1.1,
                            Time.milliseconds(2000),
                            0.1));
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }

        return env;
    }

    /**
     * Dynamic add jars
     *
     * @param jarUrls
     */
    private static void dynamicAddJars(List<String> jarUrls) {
        try {
            URL[] extraUrls =
                    jarUrls.stream()
                            .map(
                                    url -> {
                                        try {
                                            return new URL(url);
                                        } catch (MalformedURLException e) {
                                            throw new IllegalArgumentException(e);
                                        }
                                    })
                            .toArray(URL[]::new);

            URLClassLoader contextClassLoader =
                    (URLClassLoader) Thread.currentThread().getContextClassLoader();

            URL[] existingUrls = contextClassLoader.getURLs();

            if (contextClassLoader instanceof SafetyNetWrapperClassLoader) {
                // cluster run: FlinkUserCodeClassLoaders$SafetyNetWrapperClassLoader
                Method ensureInner = contextClassLoader.getClass().getDeclaredMethod("ensureInner");
                ensureInner.setAccessible(true);
                contextClassLoader = (URLClassLoader) ensureInner.invoke(contextClassLoader);
                // ! addURL is public after 1.16
                Method addURL = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                addURL.setAccessible(true);

                for (URL url : extraUrls) {
                    LOG.info("Add URL: {} to FlinkUserCodeClassloader", url);
                    addURL.invoke(contextClassLoader, url);
                }
            } else {
                // local(mini cluster) run: sun.misc.Launcher$AppClassLoader
                LOG.info("Reset {} Context Classloader", contextClassLoader);
                URLClassLoader newClassLoader =
                        new URLClassLoader(
                                ObjectArrays.concat(existingUrls, extraUrls, URL.class),
                                contextClassLoader);

                Thread.currentThread().setContextClassLoader(newClassLoader);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Load classpaths " + jarUrls + " errors", e);
        }
    }

    private EnvUtil() {}
}
