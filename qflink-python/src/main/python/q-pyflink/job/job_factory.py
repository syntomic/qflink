import importlib
from typing import Dict

from job.abstract_job import AbstractJob


class JobFactory():

    def __init__(self, config: Dict):
        self.config = config

    def create_and_run_job(self):

        name = self.config.get("job.class-name", "job.impls.demo_job.DemoJob")

        module_name = ".".join(name.split(".")[:-1])
        class_name = name.split(".")[-1]

        job: AbstractJob = getattr(importlib.import_module(module_name), class_name)(self.config)
        job.run()
