# 07 Java 调用 Python

> 原理见 [Java 调用 Python](https://syntomic.cn/contents/java-call-python)。代码在 `qflink-python/ffi-demo/`。

PyFlink 的线程模式能让 Java 算子直接调用 Python 函数，靠的是一条很朴素的调用链：Java → JNI → C → Python/C API → Python。这一章把这条链亲手搭一遍：Java 传一个日期，Python 返回它是星期几。

## 准备

- 一个 JDK（`javac` 在 PATH 上）和一个 C 编译器（macOS 上装好 Xcode 命令行工具即可）。
- 一个**带头文件和 libpython** 的 Python。默认用第 02 章 `uv sync` 建好的环境，uv 装的 Python 两样都有；也可以用 `PYTHON=/path/to/python` 指定。macOS 自带的 `/usr/bin/python3` 没有头文件，用不了。

## 跑起来

```sh
qflink-python/ffi-demo/run.sh
qflink-python/ffi-demo/run.sh 2026-10-24
```

```
4
6
```

1994 年 5 月 5 日是星期四，2026 年 10 月 24 日是星期六（星期一是 1）。

`run.sh` 做了三步，每一步对应文章里的一节：

1. `javac -h`：编译 `FFIDemo.java`，并为它的 `native` 方法生成 C 头文件；
2. `cc -shared`：用 JDK 和 Python 的头文件编译 `ffi_demo.c`，链接 libpython，产出 `libffidemo.dylib`（Linux 上是 `.so`）；
3. `java`：加载这个库时，JVM 调用其中的 `JNI_OnLoad`，Python 解释器在那里启动；之后每次调用 `dayOfWeek`，C 代码拿到 GIL，把 Java 字符串转成 Python 字符串，调用 `day_of_week`，再把结果转回 Java。

## GIL：线程越多越慢

同一个解释器里，同一时刻只有一个线程能执行 Python。`bench` 把同样的 40 万次调用平分给 1、2、4 个 Java 线程：

```sh
qflink-python/ffi-demo/run.sh bench
```

```
400000 次调用
1 个线程：1.49 秒
2 个线程：2.14 秒
4 个线程：2.36 秒
```

（Apple M5 Pro、JDK 21、Python 3.12，先预热一轮再计时，连跑三次结果相近。）线程越多反而越慢：Python 那一段只能排队，线程多了还要加上抢锁和交接的开销。

## 回到 PyFlink

第 02 章的 Python UDF 用的就是这种方式：`python.execution-mode = thread` 时，Python 解释器被 PEMJA 嵌进 Flink 的 JVM，算子线程直接调用 Python，省掉了进程模式下每条数据的跨进程序列化。代价也一样：同一个 JVM 里只有一把 GIL，多个线程调用 Python 会排队。这也解释了第 02 章 `--python` 为什么要设 PATH 和 PYTHONPATH：线程模式要找到 libpython，嵌进来的解释器要找到 pemja 和模型依赖。

## 试试看

- 传一个不合法的日期：`qflink-python/ffi-demo/run.sh 2026-13-01`。Python 抛出的 `ValueError` 先被 C 代码打印出来，再转成 Java 的 `RuntimeException: Python 函数执行失败`。

