# QFlink: Your Quest into Flink Applications

## Requirement
- Java 17

## Quick Start

- [Flink SQL](./qflink-sql/qflink-sql-sdk/src/test/java/cn/syntomic/qflink/sql/sdk/EndToEndTest.java)

## Module
- [QFlink-Common](./qflink-common/README.md): Flink Commons
- [QFlink-Rule](./qflink-rule/README.md): Rule engine
- [QFlink-Jobs](./qflink-jobs/README.md): Scenario driven jobs
- [QFlink-Python](./qflink-python/README.md): Big data with Python
- [QFlink-SQL](./qflink-sql/README.md): SQL Is All Your Need

## Branch
- flink.version

## Unix Philosophy
- Keep it simple, stupid
- Make it run, then make it right, then make it fast

## Articles
- [SQL Is All Your Need: Flink SQL](https://syntomic.cn/archives/sql-is-all-your-need-flink-sql)
- [SQL Is All Your Need: Flink SQL UDF](https://syntomic.cn/archives/sql-is-all-your-need-flink-sql-udf)
- [SQL Is All Your Need: Flink Dynamic SQL](https://syntomic.cn/archives/sql-is-all-your-need-flink-dynamic-sql)
- [Rule Engine](https://syntomic.cn/archives/flink-rule-engine)

## Dive into Deep
- Flink State：RocksDB
- Flink Checkpoint
    - Barrier travel time and alignment, addressed by Unaligned checkpoints and Buffer debloating
    - Snapshot creation time (so-called synchronous phase), addressed by asynchronous snapshots
    - Snapshot upload time (asynchronous phase), addressed by changelog

- Flink DataStream：StreamGraph
- Flink SQL：Lineage

## Data Lifecycle
- Data Generation/Collection
    - Flink CDC

- Data Storage
    - Apache Paimon

- Data Process
    - Apache Flink

- Data Application

- Data Management

## Build
- We use  flink shaded dependencies as much as possible, if you need the sources, you can clone [Flink-Shaded](https://github.com/apache/flink-shaded) project and bulid these jars locally.