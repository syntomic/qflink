import os
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

from job.abstract_job import AbstractJob


class SparkDemoJob(AbstractJob):

    def run(self):
        # Set Python environment
        os.environ['PYSPARK_PYTHON'] = "python"
        os.environ['PYSPARK_DRIVER_PYTHON'] = "python"

        spark = SparkSession.builder \
            .appName("test") \
            .config("master", "local[4]") \
            .getOrCreate()

        df = spark.createDataFrame([(5.1, 3.5, 1.4, 0.2), [5.7, 3., 4.2, 1.2]], [
                                   'sepal_length', 'sepal_width', 'petal_length', 'petal_width'])
        df.createOrReplaceTempView("source")

        # UDF
        def demo_udf(data):
            # TODO Can't get attribute 'user_eval' when execute outside the udf
            with open("../../resources/scripts/demo_script.py", "r") as f:
                code = f.read()

            exec(code, globals())
            model = user_open("../../resources/models/demo.joblib")
            return user_eval(model, data)

        spark.udf.register("demo_udf", demo_udf, returnType=IntegerType())

        # SQL
        result = spark.sql(
            "select demo_udf(array(sepal_length, sepal_width, petal_length, petal_width)) from source")
        result.show()
