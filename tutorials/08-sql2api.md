# 08 SQL2API：用 SQL 生成数据接口

> 原理见 [SQL Is All Your Need：SQL2API](https://syntomic.cn/contents/sql2api)。代码在 `qflink-api/`。

报表能只写 SQL 配出来，数据接口能不能也这样？这一章是文章里「极简实现」和「统一的请求协议」两节的可运行版本：开发者只写接口定义，参数解析、SQL 生成、结果封装由一个很小的服务完成。它不依赖 Flink，用一个内存里的 H2 数据库代替 OLAP 引擎。

## 跑起来

```sh
tutorials/sql2api.sh          # 默认端口 8090，Ctrl-C 结束
```

接口定义都在 `src/main/resources/sql2api/`：

| 文件 | 是什么 |
| --- | --- |
| `schema.sql` | 服务启动时先执行：建电影表、插几行示例数据 |
| `movie_list.sql` | 一个 SQL 模板，会变的部分写成 `{{$参数}}` |
| `movie_list.json` | 模板的路径、说明和每个参数的类型、默认值、白名单、上限 |
| `movie_query.json` | 统一请求协议能用的表和字段 |

目录里每个 `*.json` 就是一个接口：带 `sql` 的是模板接口（GET），带 `table` 的是统一请求协议接口（POST）。服务从源码目录读定义，**改了定义或新加一个，重启就生效，不用重新编译**；只有改了 Java 代码才要 `REBUILD=1 tutorials/sql2api.sh`。

## 1. SQL 模板

```sh
curl 'http://127.0.0.1:8090/api/movie/list?area=%E7%BE%8E%E5%9B%BD&order=play_cnt%20DESC&limit=2'
```

（`area` 是 URL 编码后的「美国」。）

```json
{
  "code" : 0,
  "message" : "success",
  "data" : [ {
    "title" : "坏蛋联盟",
    "cover" : "https://example.com/cover/1.png",
    "area" : "美国",
    "score" : 9.6,
    "play_cnt" : 110000000
  }, {
    "title" : "星际穿越",
    "cover" : "https://example.com/cover/2.png",
    "area" : "美国",
    "score" : 9.4,
    "play_cnt" : 86000000
  } ]
}
```

文章提醒过，**参数绝不能直接拼进 SQL**。这里每个参数按声明的类型处理：

| 类型 | 例子 | 怎么进 SQL |
| --- | --- | --- |
| `VALUE` | `area` | 变成 `?`，由数据库驱动绑定 |
| `NUMBER` | `limit`、`offset` | 也是绑定的 `?`，先检查是整数、不超过上限 |
| `IDENTIFIER` | `order` | 驱动没法绑定排序字段，只允许白名单里的值原样写进 SQL |

试试几种不怀好意的请求：

```sh
# 地区传一个注入串：它只是一个字符串，什么也查不到，而不是查出全表
curl "http://127.0.0.1:8090/api/movie/list?area=%27%20OR%20%271%27%3D%271"
# 排序里夹一句 SQL：不在白名单里，直接拒绝
curl "http://127.0.0.1:8090/api/movie/list?area=%E7%BE%8E%E5%9B%BD&order=title%3BDROP%20TABLE%20movie"
```

```json
{
  "code" : 400,
  "message" : "order must be one of [play_cnt DESC, play_cnt ASC, score DESC, score ASC], got title;DROP TABLE movie"
}
```

## 2. 统一的请求协议

模板接口每个都要写一段 SQL。再进一步，筛选、排序、分页都作为请求里的数据传进来，接口只需要声明能用哪张表、哪些字段：

```sh
curl -X POST http://127.0.0.1:8090/api/movie/query -d '{
  "filters": {"condition_operator": "AND",
              "filters": [{"field": "area", "filter_operator": "EQUAL", "value": "美国"}]},
  "order_by": [{"order_field": "play_cnt", "order_type": "DESC"}],
  "limit": 1, "offset": 0
}'
```

```json
{
  "code" : 0,
  "message" : "success",
  "data" : [ {
    "title" : "坏蛋联盟",
    "cover" : "https://example.com/cover/1.png",
    "area" : "美国",
    "style" : "喜剧",
    "score" : 9.6,
    "play_cnt" : 110000000
  } ],
  "total" : 3
}
```

字段、筛选操作符、条件连接词、排序方向都要过白名单，值一律绑定；`total` 是用同样的筛选条件另查一次的总数，供前端分页。

## 3. 测试

```sh
./mvnw -pl qflink-api -am test -Dtest=Sql2ApiTest -Dsurefire.failIfNoSpecifiedTests=false
```

`Sql2ApiTest` 覆盖了上面几种情况：正常查询、注入串只当值、排序不在白名单、分页超过上限、统一协议、协议里用了不存在的字段，还有 64 个并发请求（每个请求单独拿一个连接）。

## 没有实现的部分

文章后半讲的 DAG 编排（前置节点、关联节点、UDF 节点）没有做进这个例子。文章的结论正是：简单的数据服务用 SQL2API 很划算，一旦把重心放到编排业务逻辑上，就该交给后端了。
