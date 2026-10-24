#!/usr/bin/env bash
# Run one sql file of the tutorials from a shell, without an IDE.
#
#   tutorials/run.sh [--batch] [--python] [--rebuild] <file under qflink-sql-sdk/src/test/resources/sqls> [--key value ...]
#
#   --batch    run in batch mode, the job ends once the bounded input is read
#   --python   put the uv environment of qflink-python on the path, for the python udfs
#   --rebuild  build the java code again, needed after a java change
#
# The sql is read from the source tree (sql.read-type=file), so an edit takes effect on the next
# run without a rebuild. The run waits for the job, so a failed job ends with a non-zero code;
# a streaming job runs until Ctrl-C. Extra JVM options go in JAVA_OPTS, e.g. a louder log:
#   JAVA_OPTS=-Dorg.slf4j.simpleLogger.defaultLogLevel=warn tutorials/run.sh ...
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SDK="$ROOT/qflink-sql/qflink-sql-sdk"
RESOURCES="$SDK/src/test/resources"
CLASSPATH_FILE="$SDK/target/tutorial-classpath.txt"

mode=()
rebuild=false
python=false
while [[ $# -gt 0 && "$1" == --* ]]; do
    case "$1" in
        --batch) mode=(--execution.runtime-mode batch) ;;
        --python) python=true ;;
        --rebuild) rebuild=true ;;
        *) break ;;
    esac
    shift
done

if [[ $# -lt 1 ]]; then
    echo "usage: $0 [--batch] [--rebuild] <sql file> [--key value ...]" >&2
    echo "e.g.   $0 sql_is_all_your_need/prepare.sql" >&2
    exit 1
fi
sql="$1"
shift

if [[ ! -f "$RESOURCES/sqls/$sql" ]]; then
    echo "no such sql: $RESOURCES/sqls/$sql" >&2
    exit 1
fi

if $rebuild || [[ ! -s "$CLASSPATH_FILE" ]]; then
    echo "building qflink-sql-sdk, only needed once ..." >&2
    # The module poms inherit ${revision}, so the module is built inside the reactor (-am).
    (cd "$ROOT" && ./mvnw -q -pl qflink-sql/qflink-sql-sdk -am -DskipTests test-compile \
        org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath \
        -Dmdep.outputFile=target/tutorial-classpath.txt -Dmdep.includeScope=test)
fi

if $python; then
    venv="$ROOT/qflink-python/src/main/python/q-pyflink/.venv"
    if [[ ! -x "$venv/bin/python" ]]; then
        echo "no python environment, run: (cd qflink-python/src/main/python/q-pyflink && uv sync)" >&2
        exit 1
    fi
    # The thread mode finds libpython through the python on the path, and the embedded
    # interpreter imports pemja and the udf libraries from PYTHONPATH.
    export PATH="$venv/bin:$PATH"
    export PYTHONPATH="$(echo "$venv"/lib/python3*/site-packages)"
fi

# The job runs from the repository root like it does in an IDE, e.g. for the relative python
# files; only the sql is resolved against the test resources.
cd "$ROOT"
# Arrow, which the fluss client reads with, reaches into java.nio on JDK 17+.
opens=(--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED)
# shellcheck disable=SC2086
exec java "${opens[@]}" ${JAVA_OPTS:-} -cp "$SDK/target/test-classes:$SDK/target/classes:$(cat "$CLASSPATH_FILE")" \
    cn.syntomic.qflink.sql.sdk.FlinkSQL \
    --sql.execute-await true --sql.read-type file --file.base-dir "$RESOURCES" --sql.execute-sql "$sql" "${mode[@]+"${mode[@]}"}" "$@"
