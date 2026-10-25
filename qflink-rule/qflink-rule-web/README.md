# QFlink-Rule-Web: the console of the rule engine

A page to write the rules the [rule engine](../qflink-rule-engine/README.md) reads and to watch the alerts
it writes, in the spirit of [fraud-detection-demo](https://github.com/afedulov/fraud-detection-demo).

It is a plain java process: the jdk http server, jackson, the kafka clients and the very aviator the
engine evaluates its expressions with. **No flink, no cluster, no npm**, the page is served exactly
as it is written.

```
 browser ── REST ──►┌───────────────┐── rule event ──► rule channel ──┐
         ◄── SSE ───│qflink-rule-web│── log line ────► log channel ───┤
                    │               │                                 ▼
                    │  rule store   │                          ┌─────────────┐
                    │  (rules.json) │◄── alert ─── alert channel│ rule engine │
                    └───────────────┘◄── error ─── error channel└─────────────┘
```

The console never talks to the job directly, it only writes what the job reads and reads what the
job writes. The same page therefore drives a local run over files and a cluster over kafka, the
transport is decided by the job configuration, not by the console.

## Quick start

Everything below runs against the `develop` configuration, which needs no broker at all.

1. Start the rule engine. The simplest way is to run
   [RuleEngine](../qflink-rule-engine/src/main/java/cn/syntomic/qflink/rule/RuleEngine.java) from the IDE,
   a qfile channel is resolved against the **maven source directory** and only works from a local
   run. From a shell, build the classpath once and run it from the **repository root**:

```sh
./mvnw -o -pl qflink-rule/qflink-rule-engine -am -DskipTests compile \
    org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath \
    -Dmdep.outputFile=target/rule-classpath.txt -Dmdep.includeScope=test

java -cp "qflink-rule/qflink-rule-engine/target/classes:$(cat qflink-rule/qflink-rule-engine/target/rule-classpath.txt)" \
    cn.syntomic.qflink.rule.RuleEngine
```

> `mvn -pl qflink-rule/qflink-rule-engine exec:java` does not work here: the module poms inherit `${revision}` and the
> parent pom is installed unresolved, so the module cannot be built outside of the reactor.

2. Start the console, also from the repository root:

```sh
./mvnw -o -pl qflink-rule/qflink-rule-web -DskipTests package
java -jar qflink-rule/qflink-rule-web/target/qflink-rule-web-*.jar
```

3. Open <http://localhost:8080>, then

- **载入示例规则** in the editor gives the rule of [example-rule.json](./src/main/resources/example-rule.json),
  change the `rule_id` and press **保存并下发**
- **日志与生成器** starts the generator, the template expands `${time}`, `${int(1,10)}` and friends
- the alerts show up in **告警** and their metrics are drawn in **指标趋势**

> The rule channel of a local run is `qflink-rule/qflink-rule-engine/src/main/resources/sources/rule-source.txt`, a
> file the repository ships with one rule in it. The console appends to it, so a demo leaves it
> dirty: `git checkout qflink-rule/qflink-rule-engine/src/main/resources/sources` puts it back.

## What the page does

| Page | What it is for |
| --- | --- |
| 规则 | the rules the console manages, one card per rule: edit, 下发, 下线, 清空状态并下发, 删除 |
| 告警 | the alerts as they arrive, with the keys and the metrics of each one |
| 指标趋势 | one line per `rule_id · metric`, the value of every alert over time |
| 错误 | the error side output of the job, a rule which cannot be parsed or an expression which threw |
| 日志与生成器 | one line by hand, or a template at a chosen rate |
| 概览 | the channels, the ETL output fields and the job options the console read |

The editor has a **表单** and a **JSON** tab over the same rule, switching keeps the edit. The form
knows the shape of the engine: durations are entered in days/hours/minutes, the fields of
`etl.output-schema` are offered for the keys and the aggregate inputs, and the aggregate names are
offered as the threshold parameters.

### The store and the channel are not the same thing

The channel the engine reads is an **append only stream of rule events**, it cannot be read back:
`rules.json` under `web.data-dir` is what the page lists and edits, the channel is what the job
replays. Which means:

- a draft which does not validate never reaches the job
- a rule is only in the job once it has been **下发**, and the card says so
- **下线** publishes a `REMOVE` event and keeps the rule in the store, so it can be brought back
- **全部重新下发** replays the whole store, what a job which restarted with a fresh channel needs

### What is checked before a rule is published

The console refuses a rule the engine would only reject at runtime
([RuleValidator](./src/main/java/cn/syntomic/qflink/web/rule/RuleValidator.java)):

- `job_id` is the job of this engine, the aggregate operators drop every other one
- every expression compiles, with the same `AviatorEvaluator` the job uses, and `flat_map.pattern`
  is a regex
- **the window size is an integral multiple of the step**, otherwise it cannot be sliced
- every aggregate has an input, the engine reads `inputs[0]` whatever the method is
- the threshold only reads metrics some aggregate computes

Unknown field names are warnings, not errors: `etl.output-schema` is what the console knows, a job
which does not declare one simply gets no suggestion.

## Configuration

The console owns [web.properties](./src/main/resources/configs-develop/web.properties) and reads
**the property file of the job itself** for everything else, so the two can never drift apart.
Command line args win, unix style: `java -jar qflink-rule-web.jar --web.port 9090`.

| Option | Default | Description |
| --- | --- | --- |
| `env` | `develop` | Decides the property directory, `configs-%s` |
| `web.job-properties` | the develop job properties | The property file the rule engine job reads |
| `web.port` | `8080` | |
| `web.sources-dir` | `qflink-rule/qflink-rule-engine/src/main/resources/sources` | Where a qfile source channel lives |
| `web.sinks-dir` | `qflink-rule/qflink-rule-engine/src/main/resources/sinks` | Where a qfile sink channel lives |
| `web.data-dir` | `qflink-rule/qflink-rule-web/data` | Where the rules the console manages are kept |
| `web.history-size` | `500` | How many events a page which just opened gets |
| `web.replay` | `true` | Read a channel from the beginning, turn it off against a production topic |
| `web.static-dir` | - | Serve the page from a directory instead of the classpath, no rebuild while editing |
| `web.generator.template` | - | The log line of the generator |

The job options the console reads are `job.id`, `agg.enable`, `etl.output-schema`, `log.format`,
`log.field-time` and the `source`/`sink`/`topic`/`bootstrap.servers` of the `rule`, `log`, `alert`
and `error` channels, each under its own prefix with a fallback to the option without it, exactly
like the job reads them. Every `<channel>.properties.*` option is passed to the kafka client as is.

### Against kafka

Nothing changes on the page. Point the job at kafka, point the console at the same property file,
and the rule events are produced to the rule topic while the alerts are consumed from the alert
topic:

```properties
rule.source=kafka
rule.topic=rule-topic
rule.bootstrap.servers=broker:9092

alert.sink=kafka
alert.topic=alert-topic
alert.bootstrap.servers=broker:9092
```

> On a real topic pass `--web.replay false`, otherwise the console reads the alert topic from the
> earliest offset every time it starts. The console always consumes in a private group and never
> commits an offset, so it cannot disturb anything else which reads the same topic.

## The qfile sink

A local run used to only `print` its alerts, which nothing can read. `sink=qfile` writes one line
per record to a file the console tails
([QFileSink](../qflink-common/src/main/java/cn/syntomic/qflink/common/connectors/sink/qfile/QFileSink.java)),
the counterpart of the qfile source. It is a development sink: every record is appended and the
file is emptied when the job starts.

| Option | Default | Description |
| --- | --- | --- |
| `path` (fallback `topic`) | - | File relative to the `sinks` resource directory, or an absolute path |
| `sink-truncate` | `true` | Empty the file when the job starts |

## API

Everything the page does is one of these, they are just as usable from `curl`:

| Method | Path | What it does |
| --- | --- | --- |
| `GET` | `/api/meta` | The job, the enums, the ETL fields, the channels and the example rule |
| `GET` | `/api/status` | Open pages, event counters and the generator |
| `GET` | `/api/rules` | The stored rules with their publication state |
| `POST` | `/api/rules?publish=true` | Validate, store and optionally publish a rule |
| `POST` | `/api/rules/validate` | Validate only, nothing is stored |
| `POST` | `/api/rules/{id}/publish` | `{"state": "ACTIVE\|UPDATE\|REMOVE", "clear_state": false}` |
| `POST` | `/api/rules/publish-all` | Replay the whole store to the channel |
| `DELETE` | `/api/rules/{id}?publish=true` | Publish a REMOVE and forget the rule |
| `GET` | `/api/history?type=alert&limit=200` | What a page which just opened missed |
| `POST` | `/api/logs` | `{"line": "..."}` or `{"lines": [...]}`, the placeholders are expanded |
| `POST` | `/api/generator/start` | `{"template": "...", "rate": 2}` |
| `POST` | `/api/generator/stop` | |
| `GET` | `/api/events` | The server sent events, every frame is named `qflink` |

> An `event: error` frame would reach a browser as the failure event of its own `EventSource` and
> there is no telling the two apart there, so every frame carries one name and the page dispatches
> on the `type` of the payload.

## Generator placeholders

A **qfile** log source reads one line every `scan-interval`(2 s by default), so a generator which
runs faster than `1000 / scan-interval` lines per second only piles the lines up in the file and the
alerts fall further and further behind. The page reads the interval out of the job properties and
says what the ceiling is, next to the rate.

| Placeholder | Value |
| --- | --- |
| `${time}` | now, as `yyyy-MM-dd HH:mm:ss`, the format the log source parses |
| `${time:pattern}` | now, with an explicit pattern |
| `${seq}` | an increasing counter |
| `${uuid}` | a random uuid |
| `${int(1,10)}` | a random integer, both bounds included |
| `${double(0,1)}` | a random double |
| `${choice(a\|b\|c)}` | one of the values |

## Working on the page

Set `web.static-dir` and the console reads every file per request, a reload is then enough:

```sh
java -jar qflink-rule/qflink-rule-web/target/qflink-rule-web-*.jar --web.static-dir qflink-rule/qflink-rule-web/src/main/resources/web
```
