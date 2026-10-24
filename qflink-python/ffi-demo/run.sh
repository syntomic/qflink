#!/usr/bin/env bash
# Build the JNI + Python/C API bridge of the article 《Java 调用 Python》 and call it once.
#
#   qflink-python/ffi-demo/run.sh [yyyy-MM-dd]   call it once
#   qflink-python/ffi-demo/run.sh bench [calls]  time the calls with 1, 2 and 4 java threads
#
# Needs a JDK and a Python with its headers and a shared libpython. PYTHON picks it, by default
# the uv environment of qflink-python (uv's Python has both), else python3 on the path; the
# python3 of the Xcode command line tools has no headers.
set -euo pipefail
cd "$(dirname "$0")"

pyflink_env="../src/main/python/q-pyflink/.venv/bin/python"
PYTHON="${PYTHON:-$( [[ -x "$pyflink_env" ]] && echo "$pyflink_env" || command -v python3 )}"
py() { "$PYTHON" -c "import sysconfig; print(sysconfig.$1)"; }
py_include="$(py 'get_paths()["include"]')"
py_libdir="$(py 'get_config_var("LIBDIR")')"
py_version="$(py 'get_config_var("LDVERSION")')"
if [[ ! -f "$py_include/Python.h" ]]; then
    echo "no Python.h under $py_include, point PYTHON to a python with its headers" >&2
    exit 1
fi

JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")}"
case "$(uname -s)" in
    Darwin) platform=darwin ;;
    *) platform=linux ;;
esac
library="$(java -XshowSettings:properties -version 2>&1 | grep -q 'os.name = Mac' && echo libffidemo.dylib || echo libffidemo.so)"

# 1. Java: compile the class and generate the C header of its native method
javac -h src/main/c -d target/classes src/main/java/cn/syntomic/ffi/FFIDemo.java

# 2. C: the bridge, with the headers of the JDK and of Python, linked against libpython
cc -shared -fPIC \
    -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/$platform" \
    -I"$py_include" \
    src/main/c/ffi_demo.c \
    -L"$py_libdir" -lpython"$py_version" -Wl,-rpath,"$py_libdir" \
    -o "$library"

# 3. Run: Java -> JNI -> C -> Python/C API -> Python
java -cp target/classes cn.syntomic.ffi.FFIDemo "$@"
