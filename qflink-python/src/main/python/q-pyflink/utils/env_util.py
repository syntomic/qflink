import re
from typing import Dict

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import StreamTableEnvironment

# duration strings like "5 min", "30s", "1000 ms" or a plain number of milliseconds
_DURATION = re.compile(r"^\s*(\d+)\s*(ms|s|sec|second|seconds|min|minute|minutes|h|hour|hours)?\s*$")

_DURATION_UNIT_MS = {
    None: 1,
    "ms": 1,
    "s": 1000,
    "sec": 1000,
    "second": 1000,
    "seconds": 1000,
    "min": 60 * 1000,
    "minute": 60 * 1000,
    "minutes": 60 * 1000,
    "h": 60 * 60 * 1000,
    "hour": 60 * 60 * 1000,
    "hours": 60 * 60 * 1000,
}


class EnvUtil:
    @staticmethod
    def set_env(config: Dict) -> StreamExecutionEnvironment:
        # ! the configuration is needed by both branches
        conf = Configuration()

        EnvUtil.set_restart_config(config, conf)
        EnvUtil.set_checkpoint_config(config, conf)

        if config.get("env", "develop") == "develop":
            # open web ui
            conf.set_string("rest.port", str(config.get("rest.port", "8083")))
            env = StreamExecutionEnvironment(
                get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                    conf._j_configuration))
        else:
            env = StreamExecutionEnvironment.get_execution_environment(conf)

        env.set_max_parallelism(512)
        env.get_config().set_global_job_parameters(config)
        env.set_parallelism(int(config.get("parallelism.default", env.get_parallelism())))

        return env

    @staticmethod
    def set_table_env(env: StreamExecutionEnvironment, config: Dict = None) -> StreamTableEnvironment:
        t_env = StreamTableEnvironment.create(stream_execution_environment=env)
        # the time zone belongs to the job configuration, do not hard code it
        local_time_zone = (config or {}).get("table.local-time-zone")
        if local_time_zone:
            t_env.get_config().set_local_timezone(local_time_zone)
        return t_env

    @staticmethod
    def set_restart_config(config: Dict, conf: Configuration) -> None:
        """Same defaults as the java EnvUtil, expressed as configuration options."""
        if config.get("env", "develop") == "develop":
            # develop need not restart
            conf.set_string("restart-strategy.type", "none")
            return

        conf.set_string("restart-strategy.type", str(config.get("restart-strategy.type", "failure-rate")))
        conf.set_string("restart-strategy.failure-rate.max-failures-per-interval", "3")
        conf.set_string("restart-strategy.failure-rate.failure-rate-interval", "5 min")
        conf.set_string("restart-strategy.failure-rate.delay", "10 min")

    @staticmethod
    def set_checkpoint_config(config: Dict, conf: Configuration) -> None:
        """Checkpoint best practice, the interval accepts a flink duration string."""
        interval = config.get("execution.checkpointing.interval")
        if not interval:
            return

        conf.set_string("execution.checkpointing.interval", f"{EnvUtil.to_millis(interval)} ms")
        conf.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
        conf.set_string("execution.checkpointing.min-pause", "5 s")
        conf.set_string("execution.checkpointing.timeout", "5 min")
        conf.set_string("execution.checkpointing.tolerable-failed-checkpoints", "2")
        conf.set_string(
            "execution.checkpointing.externalized-checkpoint-retention",
            "RETAIN_ON_CANCELLATION")

    @staticmethod
    def to_millis(duration) -> int:
        """Convert a flink duration string(or a plain number of milliseconds) to milliseconds."""
        matched = _DURATION.match(str(duration))
        if matched is None:
            raise ValueError(f"cannot parse duration {duration}")

        return int(matched.group(1)) * _DURATION_UNIT_MS[matched.group(2)]
