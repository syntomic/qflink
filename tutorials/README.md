# QFlink 教程

知识花园「数据」知识库里讲原理的文章，动手部分都在这里：每一章给出要跑的命令、真实的输出，以及看输出时该注意什么。原理不在这里重复，每章开头链接到对应的文章。

## 准备

- JDK 17 或 21。Maven 不用装，仓库里的 `./mvnw` 会自己下载。
- 第一次运行会编译 Java 代码、下载依赖，要几分钟；之后每次几秒。
- 有的章节还要别的环境，在章节开头写明：Python UDF 要一个 [uv](https://docs.astral.sh/uv/) 环境，Fluss 要一个本机集群。

所有命令都在**仓库根目录**执行。

## 三个脚本

| 脚本 | 做什么 |
| --- | --- |
| `tutorials/run.sh` | 运行 `qflink-sql/qflink-sql-sdk/src/test/resources/sqls/` 下的一个 SQL 文件 |
| `tutorials/sql2api.sh` | 启动 SQL2API 示例服务 |
| `qflink-python/ffi-demo/run.sh` | 编译并运行 Java 调用 Python 的 JNI 示例 |

`run.sh` 的用法：

```sh
tutorials/run.sh [--batch] [--python] [--rebuild] <SQL 文件> [--参数 值 ...]
```

- `--batch`：批模式运行，读完有界的输入就结束。不加时是流模式，作业一直运行，按 Ctrl-C 结束。
- `--python`：把 qflink-python 的 uv 环境放进 PATH，跑 Python UDF 时用。
- `--rebuild`：改了 Java 代码之后重新编译。**只改 SQL 不用加**：SQL 直接从源码目录读取，保存就生效。
- 后面的 `--参数 值` 会成为作业配置，也会替换 SQL 里的 `${参数}`。
- 作业失败时脚本以非零退出码结束；想看 Flink 自己的日志，加 `JAVA_OPTS=-Dorg.slf4j.simpleLogger.defaultLogLevel=warn`。

大部分章节把表存在本机的 Paimon 仓库 `/tmp/paimon` 里，用下面的命令查看任意一张表：

```sh
tutorials/run.sh --batch tutorials/show.sql --table dws_metric
```

## 章节

| 章节 | 对应的文章 | 需要 |
| --- | --- | --- |
| [01 Flink SQL：一张表加工成另一张表](01-flink-sql.md) | [SQL Is All Your Need：Flink SQL](https://syntomic.cn/contents/sql-is-all-your-need-flink-sql) | — |
| [02 UDF：SQL 写不下去的时候](02-flink-sql-udf.md) | [SQL Is All Your Need：Flink SQL UDF](https://syntomic.cn/contents/sql-is-all-your-need-flink-sql-udf) | Python UDF 一节要 uv |
| [03 Dynamic SQL：把规则变成数据](03-dynamic-sql.md) | [SQL Is All Your Need：Flink Dynamic SQL](https://syntomic.cn/contents/sql-is-all-your-need-flink-dynamic-sql) | — |
| [04 回撤：看 -U / +U 怎样抵消](04-retract.md) | [Flink SQL 回撤机制与问题](https://syntomic.cn/contents/flink-sql-retract) | — |
| [05 规则引擎：在页面上下发规则](05-rule-engine.md) | [Flink 规则引擎](https://syntomic.cn/contents/flink-rule-engine) | — |
| [06 Fluss：把关联和聚合交给存储](06-fluss.md) | [Fluss：重塑实时开发](https://syntomic.cn/contents/rebuilt-streaming-development) | 本机 Fluss 集群 |
| [07 Java 调用 Python](07-java-call-python.md) | [Java 调用 Python](https://syntomic.cn/contents/java-call-python) | 带头文件的 Python |
| [08 SQL2API：用 SQL 生成数据接口](08-sql2api.md) | [SQL Is All Your Need：SQL2API](https://syntomic.cn/contents/sql2api) | — |

01、02、03 共用同一份日志数据，按顺序做最省事；其余各章可以单独做。

## 和文章不一致的地方

- 02 章的 UDTF 原来用 Hive 模块的 `json_tuple` 拆 JSON。Hive 连接器最新只发布到 Flink 1.20，在这里用的 Flink 2.2 上加载不了，改成了 Flink 自带的 `JSON_VALUE`。UDTF「一次解析、输出多列」的要点不变。
- 06 章「查不到就插入」的全局字典，在 Fluss 0.9.1 的本机集群上没有插入数据，原因还在查，见该章。
