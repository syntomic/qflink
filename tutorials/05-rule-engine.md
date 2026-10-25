# 05 规则引擎：在页面上下发规则

> 原理见 [Flink 规则引擎](https://syntomic.cn/contents/flink-rule-engine)。作业在 `qflink-rule/qflink-rule-engine/`，控制台在 `qflink-rule/qflink-rule-web/`。

第 03 章用 SQL 做动态规则，最后碰到了 SQL 的边界：告警会反复输出，做不到定时检查，规则改了以后清不掉旧状态。规则引擎回到 DataStream API 自己写算子，状态、定时器和输出都由自己控制：规则通过广播流下发，数据按每条规则动态分组，窗口按切片聚合，每条数据只累加一次。

这一章不走 `tutorials/run.sh`：规则引擎是一个 DataStream 作业，配一个网页控制台。控制台只写作业读的东西、只读作业写的东西，本机运行时两边通过文件通信，不需要 Kafka。

## 跑起来

步骤写在 [控制台的 README](../qflink-rule/qflink-rule-web/README.md#quick-start) 里，概括起来三步：

1. 在仓库根目录启动规则引擎 `cn.syntomic.qflink.rule.RuleEngine`（IDE 里直接运行最简单）。
2. 打包并启动控制台：`java -jar qflink-rule/qflink-rule-web/target/qflink-rule-web-*.jar`。
3. 打开 <http://localhost:8080>：在编辑器里「载入示例规则」、改好 `rule_id` 后「保存并下发」；在「日志与生成器」里启动数据生成器；告警出现在「告警」里，指标画在「指标趋势」里，按规则统计告警在「数据服务」里。

## 看什么

- **规则不重启就生效**：作业一直在跑，改一条规则的阈值再下发，下一个窗口就按新阈值判断。
- **切片窗口**：规则的窗口长度必须是步长的整数倍，否则下发时就会被拒绝，写进错误输出。原因是每条数据只记进一个切片，窗口的结果由切片合并而成。这一点为什么成立，见 [少算一点：大数据计算优化的统一视角](https://syntomic.cn/contents/compute-optimization-overview)。
- **清理状态**：规则带上 `clear_state` 下发，会清掉这条规则在所有分组、所有切片上攒下的状态。

## 数据服务

实时页面只能看到眼下这一屏告警，回答不了「哪条规则今天告警最多」。控制台把读到的每条告警也写进本地 H2 数据库里的一张 `alerts` 表，再用 [第 08 章的 SQL2API](./08-sql2api.md) 在这张表上定义接口，挂在同一个端口的 `/api/data/` 下。

「数据服务」页列出所有接口，填好参数就能调用：

| 接口 | 定义 | 做什么 |
| --- | --- | --- |
| `GET /api/data/alerts/count` | `alert_count.sql` 模板 | 每条规则的告警数、第一条和最后一条的时间，排序只能选白名单里的几种 |
| `POST /api/data/alerts` | `alerts.json` 表加字段白名单 | 按统一请求协议筛选、排序、分页告警明细 |

定义在 `qflink-rule/qflink-rule-web/src/main/resources/sql2api/`。再加一个接口，只要放一个定义文件、重启控制台。

这张表只是告警通道的一份拷贝：控制台默认从头读通道（`web.replay=true`），每次启动会先清空表，否则同一条告警会被记两次。

## 收尾

本机运行时，控制台会往 `qflink-rule/qflink-rule-engine/src/main/resources/sources/rule-source.txt` 追加规则，演示完用下面的命令恢复：

```sh
git checkout qflink-rule/qflink-rule-engine/src/main/resources/sources
```
