import argparse
import importlib
import json
from typing import Dict

class ConfigUtil:

    @staticmethod
    def parse_cli_args() -> Dict:
        parser = argparse.ArgumentParser(description='Arguments parser')
        _, unknown = parser.parse_known_args()

        if len(unknown) <= 1:
            return json.loads(unknown[0]) if len(unknown) == 1 else {}
        else:
            return ConfigUtil.parse_unix_style_args(unknown, parser)


    @staticmethod
    def parse_unix_style_args(unknown, parser: argparse.ArgumentParser) -> Dict:
        for arg in unknown:
            if arg.startswith(("-", "--")):
                # TODO arg with - will escape to _
                parser.add_argument(arg.split('=')[0])

        print(parser.parse_args().__dict__)

        return parser.parse_args().__dict__

    @staticmethod
    def config_with_default() -> Dict:
        command_configs = ConfigUtil.parse_cli_args()

        env = command_configs.get("env", "develop")

        # env default configs
        default_module = f"job.configs_{env}.job"
        default_configs = getattr(importlib.import_module(default_module), "configs")

        # env specific configs
        props_module = command_configs.get("props.file")
        if props_module is not None:
            specific_module = f"job.configs_{env}.{props_module}"
            specific_configs = getattr(importlib.import_module(specific_module), "configs")
        else:
            specific_configs = {}

        # command configs > specific configs > default configs
        return {**default_configs, **specific_configs, **command_configs}