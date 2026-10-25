# QFlink-Common

Shared building blocks of every QFlink job: configuration, readers, connectors and codecs.

## Configuration

A job configuration is merged from three sources, **command line args > specific property file >
default property file**([ConfigurationFactory](./src/main/java/cn/syntomic/qflink/common/configuration/ConfigurationFactory.java)):

| Option | Default | Description |
| --- | --- | --- |
| `env` | `develop` | `develop`, `test` or `produce`, decides the property directory and the local web ui |
| `props.format-dir` | `configs-%s` | Property directory, formatted with the lower case env |
| `props.default-file` | `job.properties` | Default property file, always read from the resources |
| `props.file` | - | Specific property file, read with `props.read-type` |
| `props.read-type` | `resource` | `resource` or `github` |

Command line args are parsed by [DefaultArgsParserSelector](./src/main/java/cn/syntomic/qflink/common/configuration/DefaultArgsParserSelector.java):

- no arg: the json file `example.json` in the resources is used, comments are allowed
- one arg: the arg itself is a flat json object, `java -jar job.jar '{"env":"produce"}'`
- more args: unix style, `java -jar job.jar --env produce --job.id demo`

### Options with a prefix

[QConfiguration](./src/main/java/cn/syntomic/qflink/common/configuration/QConfiguration.java) reads
an option under a prefix and falls back to the option without it, so several sources and sinks can
share one configuration:

```properties
# both sources are kafka, only the log source overrides the topic
source=kafka
log.topic=log-topic
rule.topic=rule-topic
```

`conf.get("log", TOPIC)` returns `log-topic`, `conf.get("log", SOURCE)` falls back to `kafka`.

> The derived options are built by reflection over the flink `ConfigOption` internals and cached,
> only use them while the job graph is built, never per record.

## Readers

`ReaderFactory.of(readType)` returns a reader for `resource`(classpath) or `github`
(`https://raw.githubusercontent.com/<username>/<repository>/<branch>/<path>`).

> **The github reader executes what it downloads**, e.g. the sqls of a QFlink-SQL job. Point it at a
> repository you control(`github.username`, `github.repository`, `github.branch`) and treat it as
> part of the deployment, not as a random remote source.

## Connectors

- [KafkaHelper](./src/main/java/cn/syntomic/qflink/common/connectors/helper/kafka/KafkaHelper.java):
  kafka source and sink best practice, `source`/`sink`, parallelism, watermark, delivery guarantee
  and SASL are read from the job configuration.
- [QFileSource](./src/main/java/cn/syntomic/qflink/common/connectors/source/qfile/QFileSource.java):
  a mock source which reads a file line by line, selected with `source=qfile`.

| Option | Default | Description |
| --- | --- | --- |
| `path` (fallback `topic`) | - | File relative to the `sources` resource directory, or an absolute path |
| `scan-type` (fallback `scan.startup.mode`) | `cont` | `once` bounded, `cont` keeps watching the file, `rand` emits random lines |
| `scan-interval` | `2 s` | Delay between two records |
| `scan-pause` | `0 s` | Delay before the first record, useful to let a broadcast rule arrive first |

- [QFileSink](./src/main/java/cn/syntomic/qflink/common/connectors/sink/qfile/QFileSink.java): the
  counterpart, one line per record to a file, selected with `sink=qfile`. A local run whose sink is
  `print` cannot be read by anything, one which writes a file can, e.g. by the
  [web console](../qflink-rule/qflink-rule-web/README.md).

| Option | Default | Description |
| --- | --- | --- |
| `path` (fallback `topic`) | - | File relative to the `sinks` resource directory, or an absolute path |
| `sink-truncate` | `true` | Empty the file when the job starts |

> A relative path is resolved against the **maven source directory**, so the qfile source and sink
> only work in the IDE or in a local run, not from a shaded jar on a cluster.

## Jobs

[JobEntrance](./src/main/java/cn/syntomic/qflink/common/jobs/JobEntrance.java) builds the
configuration, instantiates `job.class-name`(an [AbstractJob](./src/main/java/cn/syntomic/qflink/common/jobs/AbstractJob.java))
and runs it. A failure is rethrown, so the submitter sees a non zero exit code.
