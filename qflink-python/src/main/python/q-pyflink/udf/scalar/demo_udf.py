from pyflink.table.udf import FunctionContext, ScalarFunction, udf
from pyflink.table.types import DataTypes


class DemoUDF(ScalarFunction):

    def open(self, function_context: FunctionContext):
        # since 1.17
        model_path = function_context.get_job_parameter(
            "python.model", "./qflink-python/src/main/resources/models/demo.joblib")
        script_path = function_context.get_job_parameter(
            "python.script", "./qflink-python/src/main/resources/scripts/demo_script.py")

        with open(script_path, "r") as f:
            code = f.read()

        # ! set globals to find user functions
        exec(code, globals())

        self.model = user_open(model_path)

    def eval(self, value):
        return user_eval(self.model, value)


demo_udf = udf(DemoUDF(), result_type=DataTypes.INT())
