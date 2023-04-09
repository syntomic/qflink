# QFlink-Python: Big Data with Python

## Quick start

1. Create python virtual environment use conda
```sh
conda create --name pyflink-1.17 python=3.9 -y
conda activate pyflink-1.17
pip install -r qflink-python/src/main/python/q-pyflink/requirements.txt
```

2. [Submit job by Java](./src/main/java/cn/syntomic/qflink/python/BigDataWithPython.java)
    - Flink SQL + Python UDF
    - Flink + Pemja

3. [Submit job by Python](./src/main/python/q-pyflink/README.md): `cd qflink-python/src/main/python/q-pyflink`
    - Pyflink
    - Spark SQL + Python UDF

## Experiment

- process vs thread
- batch vs stream
    - flink self
    - flink vs spark
- java vs python submit