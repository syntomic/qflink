from abc import ABC
from typing import Dict

from pyflink.common import Types
from pyflink.table import Table
from pyflink.table.types import DataTypes
from pyflink.datastream import DataStream

from utils.env_util import EnvUtil


class AbstractJob(ABC):
    """
    The base class for all flink jobs.
    """
    def __init__(self, config:Dict) -> None:
        super().__init__()
        self.config = config

        self.env = EnvUtil.set_env(self.config)
        self.t_env = EnvUtil.set_table_env(self.env)

    def run(self) -> None:
        """Implement by user"""
        pass


    def datatype_to_typeinfo(self, table: Table):
        """Table datatype to datastream type information

        Args:
            table (Table):

        Returns:
            TypeInformation:
        """
        row_data_type = table.get_schema().to_row_data_type()
        row_type_info = []
        for field_type in row_data_type.field_types():
            if field_type == DataTypes.INT():
                row_type_info.append(Types.INT())
            elif field_type == DataTypes.STRING():
                row_type_info.append(Types.STRING())
            elif field_type == DataTypes.FLOAT():
                row_type_info.append(Types.FLOAT())
            elif field_type == DataTypes.DOUBLE():
                row_type_info.append(Types.DOUBLE())
            elif field_type == DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()):
                row_type_info.append(Types.MAP(Types.STRING(), Types.STRING()))
            elif field_type == DataTypes.ARRAY(DataTypes.STRING):
                row_type_info.append(Types.BASIC_ARRAY(Types.STRING()))
            elif field_type == DataTypes.TIMESTAMP_LTZ():
                row_type_info.append(Types.SQL_TIMESTAMP())
            else:
                raise ValueError(f"cannot convert {field_type} to typeinfo")

        return Types.ROW_NAMED(row_data_type.field_names(), row_type_info)


    def insert(self, ds: DataStream, path: str) -> None:
        """Insert datastream to table

        Args:
            ds (DataStream): datastream
            path (str): table path
        """
        # Table API
        table = self.t_env.from_data_stream(ds)
        if self.config.get("env", "develop") == "develop":
            # async execute need wait the result
            table.execute_insert(path).wait()
        else:
            table.execute_insert(path)