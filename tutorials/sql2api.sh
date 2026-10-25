#!/usr/bin/env bash
# Start the SQL2API example of qflink-sql-api on 127.0.0.1, 8090 unless a port is given.
#
#   tutorials/sql2api.sh [port]
#
# The api definitions are read from the sources, src/main/resources/sql2api: after editing one, a
# restart is enough. Only a java change needs REBUILD=1.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODULE="$ROOT/qflink-sql/qflink-sql-api"
CLASSPATH_FILE="$MODULE/target/tutorial-classpath.txt"

if [[ ! -s "$CLASSPATH_FILE" || -n "${REBUILD:-}" ]]; then
    echo "building qflink-sql-api, only needed once ..." >&2
    # The module poms inherit ${revision}, so the module is built inside the reactor (-am).
    (cd "$ROOT" && ./mvnw -q -pl qflink-sql/qflink-sql-api -am -DskipTests compile \
        org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath \
        -Dmdep.outputFile=target/tutorial-classpath.txt -Dmdep.includeScope=runtime)
fi

exec java -cp "$MODULE/target/classes:$(cat "$CLASSPATH_FILE")" cn.syntomic.qflink.sql.api.Sql2Api \
    --port "${1:-8090}" --dir "$MODULE/src/main/resources/sql2api"
