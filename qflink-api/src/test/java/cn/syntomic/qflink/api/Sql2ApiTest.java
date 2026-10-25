package cn.syntomic.qflink.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class Sql2ApiTest {

    private static Sql2Api api;

    @BeforeAll
    static void setUp() throws Exception {
        // the definitions this module ships, the same directory the standalone server reads
        Path dir = Path.of(Sql2ApiTest.class.getClassLoader().getResource("sql2api").toURI());
        api = new Sql2Api(Sql2Api.h2("mem:sql2api_test")).load(dir);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> data(Map<String, Object> response) {
        return (List<Map<String, Object>>) response.get("data");
    }

    @Test
    void testDescribe() {
        List<Map<String, Object>> apis = api.describe();
        assertEquals(2, apis.size());
        assertEquals("/movie/list", apis.get(0).get("path"));
        assertEquals("GET", apis.get(0).get("method"));
        assertEquals("/movie/query", apis.get(1).get("path"));
        assertEquals("POST", apis.get(1).get("method"));
    }

    @Test
    void testTemplate() throws Exception {
        Map<String, Object> response =
                api.get("/movie/list", Map.of("area", "美国", "order", "score DESC"));
        assertEquals(0, response.get("code"));
        List<Map<String, Object>> rows = data(response);
        assertEquals(3, rows.size());
        assertEquals("坏蛋联盟", rows.get(0).get("title"));
        assertEquals("盗梦空间", rows.get(2).get("title"));
    }

    @Test
    void testInjectionStaysAValue() throws Exception {
        // bound as a string, it matches no area instead of every row
        Map<String, Object> response = api.get("/movie/list", Map.of("area", "' OR '1'='1"));
        assertEquals(0, response.get("code"));
        assertTrue(data(response).isEmpty());
    }

    @Test
    void testOrderOutsideWhitelist() throws Exception {
        Map<String, Object> response =
                api.get("/movie/list", Map.of("area", "美国", "order", "(SELECT 1) DESC"));
        assertEquals(400, response.get("code"));
    }

    @Test
    void testLimitCapped() throws Exception {
        Map<String, Object> response =
                api.get("/movie/list", Map.of("area", "美国", "limit", "100000"));
        assertEquals(400, response.get("code"));
    }

    @Test
    void testUnknownPath() throws Exception {
        assertEquals(404, api.get("/movie/none", Map.of()).get("code"));
    }

    @Test
    void testProtocol() throws Exception {
        String body =
                "{\"filters\": {\"condition_operator\": \"AND\", \"filters\": ["
                        + "{\"field\": \"style\", \"filter_operator\": \"EQUAL\", \"value\": \"科幻\"}]},"
                        + " \"order_by\": [{\"order_field\": \"play_cnt\", \"order_type\": \"DESC\"}],"
                        + " \"limit\": 2, \"offset\": 0}";
        Map<String, Object> response = api.post("/movie/query", body);
        assertEquals(0, response.get("code"));
        assertEquals(3L, response.get("total"));
        List<Map<String, Object>> rows = data(response);
        assertEquals(2, rows.size());
        assertEquals("流浪地球", rows.get(0).get("title"));
    }

    @Test
    void testProtocolUnknownField() throws Exception {
        String body =
                "{\"filters\": {\"filters\": ["
                        + "{\"field\": \"1=1 OR title\", \"filter_operator\": \"EQUAL\", \"value\": \"x\"}]}}";
        assertEquals(400, api.post("/movie/query", body).get("code"));
    }

    @Test
    void testConcurrentRequests() throws Exception {
        // the http server answers from a thread pool, each request takes its own connection
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Map<String, Object>>> results = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                results.add(pool.submit(() -> api.get("/movie/list", Map.of("area", "美国"))));
            }
            for (Future<Map<String, Object>> result : results) {
                assertEquals(3, data(result.get()).size());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
