# PyFlink

The python environment is managed by [uv](https://docs.astral.sh/uv/), the dependencies live in
[pyproject.toml](./pyproject.toml) and are pinned in `uv.lock`.

## Quick start
```sh
cd qflink-python/src/main/python/q-pyflink

# create .venv and install the locked dependencies
uv sync

# run a job, job.class-name selects the implementation
uv run python -m job
uv run python -m job --job.class-name job.impls.demo_job.DemoJob
```

- `uv sync --extra spark` also installs pyspark, only needed by
  [spark_demo_job](./job/impls/spark_demo_job.py)
- `uv add <package>` / `uv remove <package>` update both `pyproject.toml` and `uv.lock`
- `uv export --no-hashes --format requirements-txt -o requirements.txt` if a plain requirements file
  is needed, e.g. to build a cluster image
- the python version is pinned by `.python-version`, `uv python install 3.12` if it is missing

> apache-flink must match the flink version of the java modules(root pom `flink.version`), and it
> only publishes wheels up to python 3.12.

> On apple silicon the first `uv sync` **builds apache-beam**(a transitive dependency of
> apache-flink) from source, it needs the xcode command line tools and takes a few minutes. The
> build is cached by uv, later syncs are fast.

## Python UDF in a SQL job

A java SQL job which calls a python udf needs the interpreter of this environment:

```sql
SET 'python.executable' = '<absolute path>/q-pyflink/.venv/bin/python';
SET 'python.files' = './qflink-python/src/main/python/q-pyflink/udf';
```

## Package
```sh
git add .
git commit . -m "your commit"
git archive -o jobs.zip HEAD --verbose
```
