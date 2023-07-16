# QFlink: Your Quest into Flink Applications

## Requirement
- Java 11

## Quick Start

- [Flink SQL](./qflink-sql/qflink-sql-sdk/src/test/java/cn/syntomic/qflink/sql/sdk/EndToEndTest.java)

## Module
- [QFlink-Common](./qflink-common/README.md): Commons
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
- [SQL Is All Your Need: Flink SQL](https://syntomic.github.io/2023/04/28/SQL-Is-All-Your-Need-Flink-SQL/)
- [SQL Is All Your Need: Flink SQL UDF](https://syntomic.github.io/2023/05/11/SQL-Is-All-Your-Need-Flink-SQL-UDF/)
- [SQL Is All Your Need: Flink Dynamic SQL](https://syntomic.github.io/2023/06/15/SQL-Is-All-Your-Need-Flink-Dynamic-SQL/)
- [Rule Engine](https://syntomic.github.io/2023/07/15/Flink规则引擎/)

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
