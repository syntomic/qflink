# QFlink-SQL-SDK

## Quick start

Run [EndToEndTest](./src/test/java/cn/syntomic/qflink/sql/sdk/EndToEndTest.java), it is a local
launcher(a `main`, not a junit test) which runs the sql file of `src/test/resources/example.json`.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `sql.execute-sql` | - | Sql file relative to the `sqls` resource directory |
| `sql.read-type` | `resource` | `resource` or `github` |
| `sql.execute-enable` | `true` | Whether the statement set is submitted by the TableEnvironment, set it to false when the sqls are attached to a DataStream pipeline |

- `${key}` in a sql file is replaced by the job configuration
- statements are split on `;`, `--` and `/* */` comments are removed, both are ignored inside string
  literals and quoted identifiers
- every statement is logged, the values of options which look like a secret(password, token,
  access-key, jaas config ...) are masked

## Connectors

- `qfile` source: read a file line by line, see [QFlink-Common](../../qflink-common/README.md)
- `qprint` sink: like `print` but without option validation
- `changelog-json` format: json plus a `row_kind` field(`+I`, `-U`, `+U`, `-D`)

```sql
CREATE TABLE src (key1 INT, key2 STRING) WITH (
    'connector' = 'qfile',
    'path' = 'changelog-data.txt',
    'scan-type' = 'once',
    'format' = 'changelog-json'
);
```

```json
{"row_kind":"+I","key1":10,"key2":"val1"}
{"row_kind":"-U","key1":10,"key2":"val1"}
{"row_kind":"+U","key1":15,"key2":"val1"}
```

The format reads **and** writes the row kind, so a sink using it accepts updates and deletes, and a
changelog survives a round trip. A missing `row_kind` is read as `+I`.
