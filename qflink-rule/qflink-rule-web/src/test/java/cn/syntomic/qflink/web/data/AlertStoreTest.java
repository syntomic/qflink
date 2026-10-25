package cn.syntomic.qflink.web.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import cn.syntomic.qflink.sql.api.Sql2Api;
import cn.syntomic.qflink.web.http.Json;

public class AlertStoreTest {

    // ! the definitions the console serves, read from the module sources like the console does
    private static final Path DEFINITIONS = Path.of("src/main/resources/sql2api");

    private Sql2Api.Connections database;
    private Sql2Api data;

    @BeforeEach
    void setUp(TestInfo info) throws Exception {
        database = Sql2Api.h2("mem:" + info.getDisplayName().replaceAll("\\W", ""));
        data = new Sql2Api(database).load(DEFINITIONS);
    }

    private static void alert(AlertStore store, int ruleId, String time) {
        store.insert(
                Json.read(
                        "{\"rule_id\":"
                                + ruleId
                                + ",\"alert_time\":\""
                                + time
                                + "\",\"keys\":{\"is_odd\":\"true\"},\"metrics\":{\"val_cnt\":2.0}}"));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rows(Map<String, Object> response) {
        assertEquals(0, response.get("code"), String.valueOf(response.get("message")));
        return (List<Map<String, Object>>) response.get("data");
    }

    @Test
    void testCountsAndDetails() throws Exception {
        AlertStore store = new AlertStore(database, true);
        alert(store, 100, "2023-04-09 15:42:05");
        alert(store, 100, "2023-04-09 15:44:05");
        alert(store, 101, "2023-04-09 15:43:05");

        List<Map<String, Object>> counts = rows(data.get("/alerts/count", Map.of()));
        assertEquals(2, counts.size());
        assertEquals(100, counts.get(0).get("rule_id"));
        assertEquals(2L, counts.get(0).get("alerts"));
        assertEquals("2023-04-09 15:44:05", counts.get(0).get("last_alert"));

        Map<String, Object> details =
                data.post(
                        "/alerts",
                        "{\"filters\": {\"filters\": [{\"field\": \"rule_id\","
                                + " \"filter_operator\": \"EQUAL\", \"value\": 100}]},"
                                + " \"order_by\": [{\"order_field\": \"alert_time\","
                                + " \"order_type\": \"DESC\"}]}");
        assertEquals(2L, details.get("total"));
        Map<String, Object> latest = rows(details).get(0);
        assertEquals("2023-04-09 15:44:05", latest.get("alert_time"));
        assertEquals("{\"is_odd\":\"true\"}", latest.get("alert_keys"));
    }

    @Test
    void testReplayEmptiesTheTable() throws Exception {
        alert(new AlertStore(database, true), 100, "2023-04-09 15:42:05");

        // the channel is read from its beginning again, the same alert must not be counted twice
        new AlertStore(database, true);
        assertEquals(0, rows(data.get("/alerts/count", Map.of())).size());
    }

    @Test
    void testWithoutReplayTheTableIsKept() throws Exception {
        alert(new AlertStore(database, true), 100, "2023-04-09 15:42:05");

        new AlertStore(database, false);
        assertEquals(1, rows(data.get("/alerts/count", Map.of())).size());
    }
}
