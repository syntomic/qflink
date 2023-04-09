from datetime import timedelta
from typing import Dict
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, ExternalizedCheckpointCleanup
from pyflink.java_gateway import get_gateway
from pyflink.common import Configuration, RestartStrategies
from pyflink.table import StreamTableEnvironment

class EnvUtil:
    @staticmethod
    def set_env(config:Dict) -> StreamExecutionEnvironment:

        if config.get("env", "develop") == "develop":
            # open web ui
            conf = Configuration()
            conf.set_string("rest.port", "8083")
            env = StreamExecutionEnvironment(get_gateway().jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf._j_configuration))
            # develop need not restart
        else:
            env = StreamExecutionEnvironment.get_execution_environment()
            env.set_restart_strategy(RestartStrategies.failure_rate_restart(3, timedelta(minutes=10), timedelta(minutes=1)))


        EnvUtil.set_checkpoint_config(env, config)

        env.set_max_parallelism(512)
        env.get_config().set_global_job_parameters(config)
        env.set_parallelism(conf.get_integer("parallelism.default", env.get_parallelism()))

        return env

    @staticmethod
    def set_table_env(env:StreamExecutionEnvironment) -> StreamTableEnvironment:
        t_env = StreamTableEnvironment.create(stream_execution_environment=env)
        t_env.get_config().set_local_timezone("GMT+08:00")
        return t_env

    @staticmethod
    def set_checkpoint_config(env: StreamExecutionEnvironment, config: Dict) -> None:
        # checkpoint interval in second
        if config.get("execution.checkpointing.interval"):
            env.enable_checkpointing(int(config.get("execution.checkpointing.interval")) * 1000)
            env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
            env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
            env.get_checkpoint_config().set_checkpoint_timeout(300000)
            env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)
            env.get_checkpoint_config().enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)