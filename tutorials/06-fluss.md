# 06 Fluss：把关联和聚合交给存储

> 原理见 [Fluss：重塑实时开发](https://syntomic.cn/contents/rebuilt-streaming-development)。SQL 在 `sqls/fluss/`。

用 Kafka 做实时数仓的中间层，更新、关联、聚合都只能压在 Flink 的状态里。Fluss 是面向分析的流存储，有主键表，这一章看它怎样把这些活接过去：存储层聚合、部分列更新、Delta Join，以及查不到就插入的全局字典。

## 准备：本机起一个 Fluss 集群

例子连的是 `localhost:9123`，版本要和构建里的 Fluss 客户端一致（0.9.1-incubating）。从 [Fluss 下载页](https://fluss.apache.org/downloads/) 下载这个版本的二进制包，解压后在 `conf/server.yaml` 末尾加上湖仓配置。`init.sql` 里有一张开启了湖仓的表，集群没有这几行就建不了；仓库目录和本教程其他章节一样，用 `/tmp/paimon`：

```yaml
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon
```

然后启动一个协调节点、一个数据节点和内置的 ZooKeeper：

```sh
<fluss 目录>/bin/local-cluster.sh start     # 用完后 stop
```

## 1. 建表、写入示例数据

```sh
tutorials/run.sh fluss/init.sql
tutorials/run.sh tutorials/fluss_show.sql --table fact_cdc     # 看到输出后 Ctrl-C
```

`fluss_show.sql` 从最早的位置读一张 Fluss 表的变更日志。`fact_cdc` 是主键表，第三条数据和第一条主键相同，Fluss 自己生成了 -U / +U：

```
+----+-------+--------+--------+----+
| op | dim_a |  dim_b | metric | ts |
+----+-------+--------+--------+----+
| +I |     1 |      1 |      5 |  1 |
| +I |     2 | 300000 |      2 |  2 |
| -U |     1 |      1 |      5 |  1 |
| +U |     1 |      1 |      8 |  3 |
```

用 Kafka 时，这个变更日志要靠 Flink 去重算子维护；在 Fluss 里，写入主键表就有了。

## 2. 聚合：把求和下推到存储

```sh
tutorials/run.sh fluss/merge_agg.sql                            # 十几秒后 Ctrl-C
tutorials/run.sh tutorials/fluss_show.sql --table fact_pk
```

```
+----+-------+--------+----+
| op | dim_a | metric | ts |
+----+-------+--------+----+
| +I |     1 |      5 |  1 |
| -U |     1 |      5 |  1 |
| +U |     1 |      7 |  2 |
| -U |     1 |      7 |  2 |
| +U |     1 |     15 |  3 |
```

`fact_pk` 用的是聚合合并引擎：`metric` 按主键在存储里求和，Flink 作业只管写入，不再持有聚合状态。输入就是第 04 章的 `fact.txt`，结果却是 **15（5 + 2 + 8）**，而第 04 章的 Flink SQL 先去重再求和得到的是 10。区别在于：Flink 那边把第三条当成对第一条的更新，先撤回 5；这张聚合表设了 `'table.delete.behavior' = 'ignore'`（Fluss 的聚合表默认也是这样），不处理回撤，三条都累加进去。所以存储层聚合只适合只追加的输入，原因见文章「聚合：把聚合下推到存储」一节。

## 3. 部分列更新：两条流各写各的列

```sh
tutorials/run.sh fluss/partial_update.sql                       # 十几秒后 Ctrl-C
tutorials/run.sh tutorials/fluss_show.sql --table fact_pk_dim_join
```

```
+----+-------+--------+---------+--------+--------+
| op | dim_a | metric | fact_ts |   attr | dim_ts |
+----+-------+--------+---------+--------+--------+
| +I |     1 |      5 |       1 | <NULL> | <NULL> |
| -U |     1 |      5 |       1 | <NULL> | <NULL> |
| +U |     1 |      5 |       1 |      1 |      1 |
| -U |     1 |      5 |       1 |      1 |      1 |
| +U |     1 |      5 |       1 |      2 |      2 |
| +I |     2 |      5 |       2 | <NULL> | <NULL> |
| -U |     1 |      5 |       1 |      2 |      2 |
| +U |     1 |      8 |       3 |      2 |      2 |
```

事实流只写 `metric, fact_ts`，维度流只写 `attr, dim_ts`，两边主键都是 `dim_a`，Fluss 按主键把同一行的列拼起来。双流关联的结果出来了，Flink 里却没有关联算子，也就没有两边的状态。两条流谁先到每次可能不同，中间的几行会不一样，但最后每个 `dim_a` 都是两边最新的值。前提是关联键就是两边的主键；不是的时候，用下面的 Delta Join。

## 4. Delta Join：关联键是外键

```sh
tutorials/run.sh fluss/delta_join.sql                           # 二三十秒后 Ctrl-C
tutorials/run.sh tutorials/fluss_show.sql --table fact_dim_join
```

```
+----+-------+--------+--------+---------+------+--------+
| op | dim_a |  dim_b | metric | fact_ts | attr | dim_ts |
+----+-------+--------+--------+---------+------+--------+
| +I |     1 |      1 |      8 |       3 |    1 |      1 |
| +I |     2 | 300000 |      2 |       2 |    2 |      2 |
| -U |     1 |      1 |      8 |       3 |    1 |      1 |
| +U |     1 |      1 |      8 |       3 |    3 |      3 |
| -U |     2 | 300000 |      2 |       2 |    2 |      2 |
| +U |     2 | 300000 |      2 |       2 |    2 |      2 |
| -U |     1 |      1 |      8 |       3 |    3 |      3 |
| +U |     1 |      1 |      5 |       1 |    3 |      3 |
| -U |     1 |      1 |      5 |       1 |    3 |      3 |
| +U |     1 |      1 |      8 |       3 |    3 |      3 |
```

`SET 'table.optimizer.delta-join.strategy' = 'FORCE'` 让优化器把普通的双流关联换成 Delta Join：一边来了数据，直接去另一边的 Fluss 表里按键查，Flink 不再为两边各存一份完整状态。两张表都从最早的位置读，每一次变更都会触发一次查找，所以中间会出现旧版本的组合（比如 `metric = 5` 配 `attr = 3`）；读完之后，每个 `dim_a` 关联到的都是两边的最新版本。注意它目前只支持内连接。

## 5. 全局字典：查不到就插入

`lookup.sql` 用一张带自增列的 Fluss 表做全局字典：维表关联时带上 `lookup.insert-if-not-exists`，新出现的 `dim_b` 自动插入并拿到一个整数编号。有了连续的整数编号，后面就能用 RoaringBitmap 做精确去重（原理见 [大数据去重统计](https://syntomic.cn/contents/big-data-deduplication-and-statistics)）。

```sh
tutorials/run.sh fluss/lookup.sql                               # 看到输出后 Ctrl-C
tutorials/run.sh tutorials/fluss_show.sql --table dim_mapping
```

```
+----+-------+--------+--------+----+-----------+
| op | dim_a |  dim_b | metric | ts | uid_int32 |
+----+-------+--------+--------+----+-----------+
| +I |     1 |  30000 |      5 |  1 |         1 |
| +I |     1 |      2 |      2 |  2 |         2 |
| +I |     1 |  30000 |      8 |  3 |         1 |
```

字典表里多了两行：

```
+----+-------+-----------+
| op | dim_b | uid_int32 |
+----+-------+-----------+
| +I | 30000 |         1 |
| +I |     2 |         2 |
```

`30000` 第一次出现时被插入字典、拿到 1，`2` 拿到 2，第二次出现的 `30000` 直接查到 1。字典表不会在每次运行时清空，再跑一遍，同一个 `dim_b` 还是同一个编号。

这个选项是通过 SQL 提示 `/*+ OPTIONS(...) */` 传进去的。QFlink 早先的 SQL 拆分把提示当成块注释删掉了，这一步因此一直拿到 `NULL`；现在拆分会原样保留 `/*+ ... */`。

## 6. 湖流一体：分层到 Paimon（未在本教程验证）

`fact_lake` 开启了湖仓，`tiered_backfill.sql` 演示先读 Paimon 里的历史、再接 Fluss 里的实时数据。数据要先由分层服务从 Fluss 搬到 Paimon，分层服务是一个 Flink 作业，要在 Flink 集群上提交 Fluss 发行包里的 `lib/fluss-flink-tiering-*.jar`，参数和上面 `server.yaml` 里的湖仓配置一致。这一步需要单独的 Flink 集群，本教程没有跑通验证，SQL 里的注释也提到 Flink 2.2 还读不了 `$lake` 表。
