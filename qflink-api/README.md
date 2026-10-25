# QFlink-API: serve the data a job computed

The other modules compute, this one answers: what a job wrote into an OLAP store is served as http
apis. It needs no flink, the jdk http server, jackson and an in-memory h2 standing in for the OLAP
engine are all of it.

- [Sql2Api](./src/main/java/cn/syntomic/qflink/api/Sql2Api.java): an api is a definition, not code,
  either a sql template with typed parameters or a table with the fields the
  [unified request protocol](./src/main/java/cn/syntomic/qflink/api/QueryProtocol.java) may use
- [sql2api](./src/main/resources/sql2api): the movie example, `schema.sql` plus one json per api

```sh
tutorials/sql2api.sh          # http://127.0.0.1:8090/api/movie/list
```

The definitions are read from the sources, a restart picks up an edit. The walk through is
[tutorial 08](../tutorials/08-sql2api.md).
