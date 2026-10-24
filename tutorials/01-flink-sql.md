# 01 Flink SQL：一张表加工成另一张表

> 原理见 [SQL Is All Your Need：Flink SQL](https://syntomic.cn/contents/sql-is-all-your-need-flink-sql)。SQL 在 `sqls/sql_is_all_your_need/`。

这一章用 Paimon 存表、Flink SQL 加工，走完一条链路：日志入库、清洗成明细、按窗口统计、超阈值告警、复杂事件匹配。中间会看到同一段 SQL 在批模式和流模式下给出不同的结果，以及原因。

## 1. 准备：一张日志表

```sh
tutorials/run.sh sql_is_all_your_need/prepare.sql
tutorials/run.sh --batch tutorials/show.sql --table ods_log
```

`prepare.sql` 在 `/tmp/paimon` 里建好 `ods_log`，写进 5 条原始日志，每条是「时间 关键字 JSON」：

```
+----+--------------------------------+
| op |                            log |
+----+--------------------------------+
| +I | 2023-04-09 15:40:05 stdout ... |
| +I | 2023-04-09 15:41:05 stdout ... |
| +I | 2023-04-09 15:42:05 stdout ... |
| +I | 2023-04-09 15:43:05 stdout ... |
| +I | 2023-04-09 15:44:05 stdout ... |
+----+--------------------------------+
```

`prepare.sql` 会先删掉本章所有的表，想从头再来就重跑它。

## 2. ETL：把日志拆成字段

```sh
tutorials/run.sh --batch sql_is_all_your_need/flink_sql/etl.sql
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

（输出里的列宽为了排版压缩过，下同。）

`dwd_log` 上定义了事件时间 `rowtime` 和水位线：`rowtime - 10 秒`，也就是允许数据最多迟到 10 秒。批模式下 `SELECT *` 碰到这种时间属性列会报 Flink 的规划错误，所以这里用 `--columns` 列出了其余几列。

## 3. 分析：一行切换批和流

`analysis.sql` 按 1 分钟的滚动窗口，统计每个窗口里 `key2` 有几个不同的值。先用批模式跑：

```sh
tutorials/run.sh --batch sql_is_all_your_need/flink_sql/analysis.sql
tutorials/run.sh --batch tutorials/show.sql --table dws_metric
```

```
+----+---------------------+---------------------+--------+--------+
| op |        window_start |          window_end |    dim | metric |
+----+---------------------+---------------------+--------+--------+
| +I | 2023-04-09 15:40:00 | 2023-04-09 15:41:00 | stdout |      1 |
| +I | 2023-04-09 15:41:00 | 2023-04-09 15:42:00 | stdout |      1 |
| +I | 2023-04-09 15:42:00 | 2023-04-09 15:43:00 | stdout |      1 |
| +I | 2023-04-09 15:43:00 | 2023-04-09 15:44:00 | stdout |      1 |
| +I | 2023-04-09 15:44:00 | 2023-04-09 15:45:00 | stdout |      1 |
+----+---------------------+---------------------+--------+--------+
```

再从头来一遍，这次用流模式跑分析。流模式的作业不会自己结束，等十几秒、看到它提交之后按 Ctrl-C：

```sh
tutorials/run.sh sql_is_all_your_need/prepare.sql
tutorials/run.sh --batch sql_is_all_your_need/flink_sql/etl.sql
tutorials/run.sh sql_is_all_your_need/flink_sql/analysis.sql      # 十几秒后 Ctrl-C
tutorials/run.sh --batch tutorials/show.sql --table dws_metric
```

```
+----+---------------------+---------------------+--------+--------+
| op |        window_start |          window_end |    dim | metric |
+----+---------------------+---------------------+--------+--------+
| +I | 2023-04-09 15:40:00 | 2023-04-09 15:41:00 | stdout |      1 |
| +I | 2023-04-09 15:41:00 | 2023-04-09 15:42:00 | stdout |      1 |
| +I | 2023-04-09 15:42:00 | 2023-04-09 15:43:00 | stdout |      1 |
+----+---------------------+---------------------+--------+--------+
```

**同一段 SQL，流模式少了两个窗口。**最后一条日志是 15:44:05，水位线等于它减 10 秒，停在 15:43:55。流模式只有在水位线越过窗口终点时才输出这个窗口，15:43–15:44 和 15:44–15:45 这两个窗口还在等可能迟到的数据。批模式知道输入已经读完，所有窗口直接输出。

这就是文章里「一行切换成批处理」的意思：修数、回溯时用批模式跑同一段 SQL，结果和流模式最终一致，但不用等水位线。

## 4. 事件驱动：超阈值就告警

```sh
tutorials/run.sh --batch sql_is_all_your_need/flink_sql/event_driven.sql
tutorials/run.sh --batch tutorials/show.sql --table dws_alert
```

```
+----+--------------+--------+---------------------+--------+
| op | window_start |    key |                time | metric |
+----+--------------+--------+---------------------+--------+
| +I |     20230409 | stdout | 2023-04-09 15:44:05 |     21 |
+----+--------------+--------+---------------------+--------+
```

按天累加 `key1`（5 + 4 + 1 + 5 + 6 = 21），超过 10 就写一条告警。这条 SQL 没有窗口，在流模式下每来一条数据结果就更新一次，下游会收到「撤回旧值、写入新值」两条消息，这就是第 04 章讲的回撤。

## 5. 复杂事件处理：MATCH_RECOGNIZE

```sh
tutorials/run.sh --batch sql_is_all_your_need/flink_sql/cep.sql
```

```
+----+----------+-------------------------+-------------------------+-------------------------+
| op | key_word |                start_ts |               bottom_ts |                  end_ts |
+----+----------+-------------------------+-------------------------+-------------------------+
| +I |   stdout | 2023-04-09 15:40:05.000 | 2023-04-09 15:42:05.000 | 2023-04-09 15:43:05.000 |
+----+----------+-------------------------+-------------------------+-------------------------+
```

模式是「一个起点、连续下降、再上升」，也就是一个 V 形：`key1` 从 5 降到 4、再降到 1，然后升到 5。匹配到的起点、谷底、终点分别是 15:40:05、15:42:05、15:43:05。

## 试试看

- 改 `prepare.sql` 里的日志，比如把最后一条的时间改成 15:50:05，再用流模式跑分析：水位线推进到 15:49:55，哪些窗口会补出来？
- 打开 `analysis.sql` 里被注释掉的 `table.optimizer.distinct-agg.split.enabled`，把 `INSERT INTO dws_metric` 改成 `EXPLAIN INSERT INTO dws_metric`，用流模式运行（打印完计划就结束）。在 `Optimized Physical Plan` 里能看到去重被拆成了两层：先按 `MOD(HASH_CODE(key2), 1024)` 分桶各自去重，再把各桶的计数用 `$SUM0` 加起来。同一个 `key2` 值只会落进一个桶，所以各桶的计数能直接相加。
