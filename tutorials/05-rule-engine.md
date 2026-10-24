# 05 规则引擎：在页面上下发规则

> 原理见 [Flink 规则引擎](https://syntomic.cn/contents/flink-rule-engine)。作业在 `qflink-rule/`，控制台在 `qflink-web/`。

第 03 章用 SQL 做动态规则，最后碰到了 SQL 的边界：告警会反复输出，做不到定时检查，规则改了以后清不掉旧状态。规则引擎回到 DataStream API 自己写算子，状态、定时器和输出都由自己控制：规则通过广播流下发，数据按每条规则动态分组，窗口按切片聚合，每条数据只累加一次。

这一章不走 `tutorials/run.sh`：规则引擎是一个 DataStream 作业，配一个网页控制台。控制台只写作业读的东西、只读作业写的东西，本机运行时两边通过文件通信，不需要 Kafka。

## 跑起来

步骤写在 [qflink-web 的 README](../qflink-web/README.md#quick-start) 里，概括起来三步：

1. 在仓库根目录启动规则引擎 `cn.syntomic.qflink.rule.RuleEngine`（IDE 里直接运行最简单）。
2. 打包并启动控制台：`java -jar qflink-web/target/qflink-web-*.jar`。
3. 打开 <http://localhost:8080>：在编辑器里「载入示例规则」、改好 `rule_id` 后「保存并下发」；在「日志与生成器」里启动数据生成器；告警出现在「告警」里，指标画在「指标趋势」里。

## 看什么

- **规则不重启就生效**：作业一直在跑，改一条规则的阈值再下发，下一个窗口就按新阈值判断。
- **切片窗口**：规则的窗口长度必须是步长的整数倍，否则下发时就会被拒绝，写进错误输出。原因是每条数据只记进一个切片，窗口的结果由切片合并而成。这一点为什么成立，见 [少算一点：大数据计算优化的统一视角](https://syntomic.cn/contents/compute-optimization-overview)。
- **清理状态**：规则带上 `clear_state` 下发，会清掉这条规则在所有分组、所有切片上攒下的状态。

## 收尾

本机运行时，控制台会往 `qflink-rule/src/main/resources/sources/rule-source.txt` 追加规则，演示完用下面的命令恢复：

```sh
git checkout qflink-rule/src/main/resources/sources
```
