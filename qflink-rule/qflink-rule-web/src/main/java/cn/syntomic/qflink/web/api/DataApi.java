package cn.syntomic.qflink.web.api;

import java.util.Map;

import cn.syntomic.qflink.sql.api.Sql2Api;
import cn.syntomic.qflink.web.http.Ctx;
import cn.syntomic.qflink.web.http.Json;
import cn.syntomic.qflink.web.http.Router;

/**
 * The data service of the console: every SQL2API definition is mounted under {@code /api/data}.
 *
 * <p>The console does not know what the apis are, it only routes to them, so a new api is one more
 * definition file and a restart, no java.
 */
public class DataApi {

    public static final String PREFIX = "/api/data";

    private final Sql2Api data;

    public DataApi(Sql2Api data) {
        this.data = data;
    }

    public void bind(Router router) {
        router.route("GET", PREFIX, this::list);
        for (Map<String, Object> api : data.describe()) {
            String path = (String) api.get("path");
            if ("GET".equals(api.get("method"))) {
                router.route(
                        "GET", PREFIX + path, ctx -> reply(ctx, data.get(path, ctx.queries())));
            } else {
                router.route(
                        "POST",
                        PREFIX + path,
                        ctx -> reply(ctx, data.post(path, Json.write(ctx.body()))));
            }
        }
    }

    /** What the data service offers, the page lists it and lets one try every api. */
    private void list(Ctx ctx) throws Exception {
        ctx.json(Json.of("prefix", PREFIX, "apis", data.describe()));
    }

    /** SQL2API answers with its own code, 0 is a success. */
    private static void reply(Ctx ctx, Map<String, Object> response) throws Exception {
        int code = (int) response.get("code");
        ctx.json(code == 0 ? 200 : code, response);
    }
}
