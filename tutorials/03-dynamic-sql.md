# 03 Dynamic SQL：把规则变成数据

> 原理见 [SQL Is All Your Need：Flink Dynamic SQL](https://syntomic.cn/contents/sql-is-all-your-need-flink-dynamic-sql)。SQL 在 `sqls/sql_is_all_your_need/flink_dynamic_sql/`，数据在 `src/test/resources/sources/common-data.txt`。

一条监控规则一句 SQL 就能写完，规则多了怎么办？这一章跟着需求走四步：一条规则、两条规则、把规则存进表、规则表变成一条流。每一步的作业都是流作业，读的是一个本地文件，看到输出后按 Ctrl-C。

输出里每行开头的 `6>`、`11>` 是打印它的子任务编号，和结果无关。

## 准备：一张规则表

```sh
tutorials/run.sh sql_is_all_your_need/flink_dynamic_sql/prepare.sql
```

`dim_rule` 是一张 Paimon 表，存了两条规则，每条拆成过滤、分组、窗口、聚合、阈值五个要素：

| 规则 | 过滤 | 分组 | 窗口 | 聚合 | 阈值 |
| --- | --- | --- | --- | --- | --- |
| 1 | `key_word` 是 stdout | `key2` | 天 | `key1` 计数 | 大于 1 |
| 2 | 无 | `key_word` | 小时 | `key2` 去重计数 | 大于 2 |

（表里的 `EXPR$0`、`EXPR$1`、`EXPR$2` 指的是数据字段 `key_word`、`key1`、`key2`。）

## 阶段一：只有一条规则

```sh
tutorials/run.sh sql_is_all_your_need/flink_dynamic_sql/step1_start.sql
```

```
6> +U[1, 2023-04-09 00:00:00, val2, 2023-04-09 15:42:05, {key1=2.0}]
```

规则 1 直接写成一句 GROUP BY + HAVING：`val2` 当天出现了 2 次，超过阈值 1。

## 阶段二：规则开始增加

```sh
tutorials/run.sh sql_is_all_your_need/flink_dynamic_sql/step2_grow.sql
```

```
6> +U[1, 2023-04-09 00:00:00, val2, 2023-04-09 15:42:05, {key1_cnt=2.0}]
11> +U[2, 2023-04-09 15:00:00, stdout, 2023-04-09 15:43:05, {key2_cnt=3.0}]
11> -U[2, 2023-04-09 15:00:00, stdout, 2023-04-09 15:43:05, {key2_cnt=3.0}]
11> +U[2, 2023-04-09 15:00:00, stdout, 2023-04-09 15:44:05, {key2_cnt=4.0}]
```

第二条规则用 UNION ALL 接上。规则 2 的结果先是 3，来了 `val4` 之后变成 4：下游先收到撤回旧值的 `-U`，再收到新值 `+U`。规则每加一条，SQL 就长一段，作业就得重新上线一次。

## 阶段三：把规则变成数据

```sh
tutorials/run.sh sql_is_all_your_need/flink_dynamic_sql/step3_explode.sql
```

```
1> +U[1, 2023-04-09 00:00:00, val2, 2023-04-09 15:42:05, {key1_cnt=2.0}]
15> +U[2, 2023-04-09 15:00:00, stdout, 2023-04-09 15:43:05, {key2_cnt=3.0}]
15> -U[2, 2023-04-09 15:00:00, stdout, 2023-04-09 15:43:05, {key2_cnt=3.0}]
15> +U[2, 2023-04-09 15:00:00, stdout, 2023-04-09 15:44:05, {key2_cnt=4.0}]
```

**结果和阶段二完全一样，但 SQL 里已经没有任何一条具体的规则了。**每条数据通过维表关联，和 `dim_rule` 里的每条规则拼成一行（也就是文章说的「炸开」），再由 `dynamic_filter`、`dynamic_key`、`dynamic_window`、`dynamic_agg` 四个 UDF 按规则解释执行。加规则只需要往 `dim_rule` 插一行。

## 阶段四：规则表变成一条流

```sh
tutorials/run.sh sql_is_all_your_need/flink_dynamic_sql/step4_challenge.sql
```

输出和阶段三一样，只改了关联方式。阶段三用 `FOR SYSTEM_TIME AS OF` 做维表关联，每来一条日志都要去规则表查一次，日志和规则一多，查询就成了瓶颈。阶段四去掉了这一句，规则表变成一条流：Paimon 先把规则全量读进 Flink，之后持续捕获变更，规则存在 Flink 的状态里，日志来了在本地匹配。

到这里看起来已经够用了，但文章最后一节列了上线后 SQL 绕不过去的三件事：同一周期内每次更新都会输出，达到阈值后会反复告警；做不到「每天定时检查一次」这类按时间触发的逻辑；规则修改后，之前累积的状态没法精细地清理。这些都要对状态、定时器和输出做精细控制，超出了 SQL 的能力，要换成第 05 章用 DataStream API 写的规则引擎。

## 试试看

- 阶段三运行时，另开一个终端往 `dim_rule` 插一条新规则（照着 `prepare.sql` 写一句 INSERT，用 `tutorials/run.sh` 执行），看新规则能不能在不重启作业的情况下生效。
