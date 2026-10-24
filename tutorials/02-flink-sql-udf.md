# 02 UDF：SQL 写不下去的时候

> 原理见 [SQL Is All Your Need：Flink SQL UDF](https://syntomic.cn/contents/sql-is-all-your-need-flink-sql-udf)。SQL 在 `sqls/sql_is_all_your_need/flink_sql_udf/`，Java UDF 在 `qflink-sql/qflink-sql-udf/`。

三类 UDF 各解决一个问题：表值函数（UDTF）让一次正则解析输出多列；聚合函数（UDAF）把 HyperLogLog 引进 SQL，算累计 UV；Python UDF 把机器学习模型接进实时流。

## 1. 表值函数：一次解析，输出多列

第 01 章的 ETL 为了拿三个字段，把同一个正则跑了三遍。`QRegexExtract` 跑一遍，按捕获组输出三列：

```sh
tutorials/run.sh sql_is_all_your_need/prepare.sql
tutorials/run.sh --batch sql_is_all_your_need/flink_sql_udf/etl_udtf.sql
tutorials/run.sh --batch tutorials/show.sql --table dwd_log --columns '`time`, key_word, key1, key2'
```

```
+----+---------------------+----------+------+------+
| op |                time | key_word | key1 | key2 |
+----+---------------------+----------+------+------+
| +I | 2023-04-09 15:40:05 |   stdout |    5 | val1 |
| +I | 2023-04-09 15:41:05 |   stdout |    4 | val2 |
| +I | 2023-04-09 15:42:05 |   stdout |    1 | val2 |
| +I | 2023-04-09 15:43:05 |   stdout |    5 | val3 |
| +I | 2023-04-09 15:44:05 |   stdout |    6 | val4 |
+----+---------------------+----------+------+------+
```

结果和第 01 章一样，区别在计算量：正则从每行三次变成了一次。

> 文章里拆 JSON 用的是 Hive 模块的 `json_tuple`。Hive 连接器最新只发布到 Flink 1.20，这里用的是 Flink 2.2，加载不了，所以改成了 Flink 自带的 `JSON_VALUE`。

## 2. 聚合函数：用 HyperLogLog 算累计 UV

「从当天 0 点到现在有几个不同的 `key2`」是一个累计去重。`analysis_hll.sql` 先按分钟把 `key2` 聚成一个 HyperLogLog，再用 `OVER` 窗口把每分钟的 HyperLogLog 逐个合并、估计基数：

```sh
tutorials/run.sh --batch sql_is_all_your_need/flink_sql_udf/analysis_hll.sql
```

```
+----+---------------------+---------------------+--------+--------+
| op |        window_start |          window_end |    dim | metric |
+----+---------------------+---------------------+--------+--------+
| +I | 2023-04-09 00:00:00 | 2023-04-09 15:41:00 | stdout |      1 |
| +I | 2023-04-09 00:00:00 | 2023-04-09 15:42:00 | stdout |      2 |
| +I | 2023-04-09 00:00:00 | 2023-04-09 15:43:00 | stdout |      2 |
| +I | 2023-04-09 00:00:00 | 2023-04-09 15:44:00 | stdout |      3 |
| +I | 2023-04-09 00:00:00 | 2023-04-09 15:45:00 | stdout |      4 |
+----+---------------------+---------------------+--------+--------+
```

对照数据：`val1`、`val2`、`val2`、`val3`、`val4`，第三分钟又来了一次 `val2`，所以累计值停在 2。

能这样逐分钟合并，是因为 HyperLogLog 能合并；但它不能撤销，所以近 N 天的 UV 不能用「减掉 N 天前那天」的递推，只能把 N 份合并。为什么，见 [少算一点：大数据计算优化的统一视角](https://syntomic.cn/contents/compute-optimization-overview)。

## 3. Python UDF：把模型接进实时流

先准备 Python 环境（只需一次）：

```sh
(cd qflink-python/src/main/python/q-pyflink && uv sync)
```

`python_udf.sql` 用 datagen 每秒造一条鸢尾花数据，交给 Python 写的 `demo_udf` 分类。这是流作业，看到几行输出后按 Ctrl-C：

```sh
tutorials/run.sh --python sql_is_all_your_need/flink_sql_udf/python_udf.sql
```

```
+I[0]
+I[2]
+I[0]
+I[2]
+I[1]
```

每行是模型给出的类别。数据是随机生成的，你看到的序列会不一样。

SQL 里设了 `python.execution-mode = thread`：Python 解释器嵌在 JVM 里，算子线程直接调用 Python 函数，省掉跨进程的序列化。`--python` 做了两件事：把 uv 环境里的 `python` 放进 PATH（线程模式靠它找到 libpython），把环境的 `site-packages` 放进 PYTHONPATH（嵌入的解释器从这里导入 pemja 和模型依赖）。少了任何一个，作业都会在启动后失败，脚本以非零退出码结束。线程模式背后的原理，见第 07 章。

## 试试看

- 在 `prepare.sql` 里多加几条不同 `key2` 的日志，看 HyperLogLog 的估计值和真实值差多少。HyperLogLog 的误差随桶的个数怎么变，可以在 [大数据去重统计](https://syntomic.cn/contents/big-data-deduplication-and-statistics) 里的实验调。
- 把 `python.execution-mode` 改成 `process`，对比两种模式启动和处理的速度。
