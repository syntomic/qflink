from pyflink.datastream import RuntimeContext, ProcessFunction
from pyflink.common import Row

from job.abstract_job import AbstractJob


class DemoJob(AbstractJob):

    def run(self):
        self.create_tables()

        source = self.t_env.to_data_stream(self.t_env.from_path("source"))
        transform = source.process(MyProcessFunction(), self.datatype_to_typeinfo(self.t_env.from_path("sink")))
        self.t_env.from_data_stream(transform).execute_insert("sink").wait()


    def create_tables(self):
        self.t_env.execute_sql("""
            CREATE TABLE IF NOT EXISTS `origin` (
                `sepal_length` FLOAT,
                `sepal_width` FLOAT,
                `petal_length` FLOAT,
                `petal_width` FLOAT
            ) WITH (
                'connector' = 'datagen',
                'rows-per-second' = '1',
                'fields.sepal_length.max' = '8',
                'fields.sepal_length.min' = '4',
                'fields.sepal_width.max' = '5',
                'fields.sepal_width.min' = '2',
                'fields.petal_length.max' = '7',
                'fields.petal_length.min' = '1',
                'fields.petal_width.max' = '3',
                'fields.petal_width.min' = '0'
            )
        """)

        self.t_env.execute_sql("""
            CREATE TABLE IF NOT EXISTS `sink` (
                `classifier` INT
            ) WITH (
                'connector' = 'print'
            )
        """)

        self.t_env.execute_sql("""
            CREATE VIEW `source` AS
            SELECT
                ARRAY[`sepal_length`, `sepal_width`, `petal_length`, `petal_width`]
            FROM
                `origin`
        """)


class MyProcessFunction(ProcessFunction):

    def open(self, runtime_context: RuntimeContext):

        script = runtime_context.get_job_parameter("python.script", "../../resources/scripts/demo_script.py")
        arg = runtime_context.get_job_parameter("python.model", "../../resources/models/demo.joblib")

        with open(script, "r") as f:
            code = f.read()
        exec(code, globals())

        self.model = user_open(arg)


    def process_element(self, value: Row, ctx: 'ProcessFunction.Context'):
        # ! must return iterable object
        return [Row(user_eval(self.model, value))]