# QFlink-Python: Big Data with Python

## Quick start

1. Create the python virtual environment with [uv](https://docs.astral.sh/uv/)
```sh
cd qflink-python/src/main/python/q-pyflink
uv sync
```

2. [Submit job by Java](./src/main/java/cn/syntomic/qflink/python/BigDataWithPython.java)
    - Flink SQL + Python UDF
    - Flink + Pemja

    Both need the interpreter of the virtual environment, `python.pythonpath` must point to its
    site-packages, otherwise the embedded interpreter cannot even import pemja:
```json
{
    "python.executable": "<abs path>/q-pyflink/.venv/bin/python",
    "python.pythonpath": "<abs path>/q-pyflink/.venv/lib/python3.12/site-packages"
}
```
> ! A pemja interpreter is bound to the thread which creates it, an operator must create it in the
> method which uses it, not in `open()`: they are not always the same thread.

3. [Submit job by Python](./src/main/python/q-pyflink/README.md): `cd qflink-python/src/main/python/q-pyflink`
    - Pyflink
    - Spark SQL + Python UDF

## Experiment

- process vs thread
- batch vs stream
    - flink self
    - flink vs spark
- java vs python submit