# 04 回撤：看 -U / +U 怎样抵消

> 原理见 [Flink SQL 回撤机制与问题](https://syntomic.cn/contents/flink-sql-retract)。SQL 在 `sqls/retract/` 和 `sqls/ptfs/`，数据在 `src/test/resources/sources/retract/`，自定义算子在 `qflink-sql/qflink-sql-udf/.../ptfs/`。

流式 SQL 的结果会随输入不断变化，一次更新会拆成「撤回旧值」（-U）和「写入新值」（+U）两条消息。这保证了最终结果正确，代价是中间可能露出错误的结果。这一章先复现文章里的两种中间错误结果，再用 Process Table Function（PTF）写的自定义算子把它们挡住。

几个例子都是流作业，读本地文件，看到输出后按 Ctrl-C。

## 现象一：聚合时指标下降

数据 `fact.txt` 是同一个 `dim_a` 下的三条记录，第三条更新了第一条（主键是 `dim_a, dim_b`）：

```
{"dim_a":1,"dim_b":30000,"metric":5,"ts":1}
{"dim_a":1,"dim_b":2,"metric":2,"ts":2}
{"dim_a":1,"dim_b":30000,"metric":8,"ts":3}
```

`agg_down.sql` 先按主键去重取最新，再按 `dim_a` 求和：

```sh
tutorials/run.sh retract/agg_down.sql
```

```
+----+-------+--------+----+
| op | dim_a | metric | ts |
+----+-------+--------+----+
| +I |     1 |      5 |  1 |
| -U |     1 |      5 |  1 |
| +U |     1 |      7 |  2 |
| -U |     1 |      7 |  2 |
| +U |     1 |      2 |  2 |
| -U |     1 |      2 |  2 |
| +U |     1 |     10 |  3 |
+----+-------+--------+----+
```

正确的结果是 5 → 7 → 10。但第三条记录到达时，去重算子先发出「撤回 5」，求和算子收到后从 7 减掉 5，**先输出了一个错误的 2**，收到「加上 8」之后才变成 10。监控大盘上，一个本该只增不减的指标会在这里往下掉一下。

## 现象二：关联时关系为空

`join_null.sql` 是一张事实表左关联一张维表，维表 `dim.txt` 里同一个 `dim_a` 先后有两个版本（attr 从 1 改成 2）：

```sh
tutorials/run.sh retract/join_null.sql
```

```
13> +I(1,1,5,1,null,null)
13> -D(1,1,5,1,null,null)
13> +I(1,1,5,1,1,1)
13> -U(1,1,5,1,1,1)
13> +I(1,1,5,1,null,null)
13> -D(1,1,5,1,null,null)
13> +I(1,1,5,1,2,2)
```

维表更新时，关联算子先收到「撤回 attr=1」，于是撤回已经关联上的结果，并补发一条「没关联上」的 `null`；再收到 attr=2，才输出新的关联结果。中间那条 `(…, null, null)` 就是文章说的「下游查询时突然看不到数据」。（开头第一对 `+I null / -D null` 是事实先于维表到达，属于正常的左关联行为。）

## 再看一个：分组键变了

`agg_delete.sql` 按 `attr` 分组，而 `fact_join.txt` 里同一条记录的 `attr` 从 1 改成了 2：

```sh
tutorials/run.sh retract/agg_delete.sql
```

```
+----+------+--------+----+
| op | attr | metric | ts |
+----+------+--------+----+
| +I |    1 |      5 |  1 |
| -D |    1 |      5 |  1 |
| +I |    2 |      5 |  2 |
+----+------+--------+----+
```

记录换了组，旧组的那一行被删掉（-D），新组插入一行。如果下游只认插入、忽略删除，`attr = 1` 这一组就会永远留着一个过期的 5。

## 用 PTF 挡住中间结果

Flink 2.1 起，可以在 SQL 里用 Process Table Function 写自定义算子：它能拿到每条消息的变更类型，也能用自己的状态。

**自定义聚合**：收到 -U 时只更新内部状态、不输出，只在 +I / +U 时输出：

```sh
tutorials/run.sh ptfs/ignore_retract_agg.sql
```

```
+----+-------+--------+----+
| op | dim_a | metric | ts |
+----+-------+--------+----+
| +I |     1 |      5 |  1 |
| +I |     1 |      7 |  2 |
| +I |     1 |     10 |  3 |
+----+-------+--------+----+
```

那个错误的 2 不见了，输出就是 5 → 7 → 10。

**自定义关联**：维表进关联之前，先丢掉它的 -U，只保留 +I / +U：

```sh
tutorials/run.sh ptfs/ignore_retract.sql
```

```
13> +I(1,1,5,1,null,null)
13> -D(1,1,5,1,null,null)
13> +I(1,1,5,1,1,1)
13> +I(1,1,5,1,2,2)
```

维表从 attr=1 改成 2 时，直接输出新的关联结果，中间不再出现 `null`。结果表以 `dim_a` 为主键，后一条覆盖前一条。

## 代价

PTF 能挡住中间结果，是因为它不再遵守「先撤回、再更新」的约定：输出变成了一条按主键覆盖的流，下游必须是能按主键更新的存储。文章最后的对比表里还有另外两种做法：开 MiniBatch（只能缓解）、写入时用版本列挡住（只能按一个字段比较）。

## 试试看

- 在 `agg_down.sql` 前面加上 `SET 'table.exec.mini-batch.enabled' = 'true';`、`SET 'table.exec.mini-batch.allow-latency' = '5 s';`、`SET 'table.exec.mini-batch.size' = '1000';`，看那个错误的 2 还会不会出现。
