package cn.syntomic.qflink.web.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.sql.api.Sql2Api.Connections;
import cn.syntomic.qflink.web.http.Json;

/**
 * Copies every alert the console reads from the alert channel into the {@code alerts} table, so the
 * data service can answer questions the live feed cannot, e.g. how many alerts a rule raised today.
 *
 * <p>The table is created by the {@code schema.sql} of the data service definitions. It is a copy
 * of the channel, not a second source of truth: when the channel is read from its beginning the
 * table is emptied first, otherwise a restart would count every alert twice.
 */
public class AlertStore {

    private static final Logger LOG = LoggerFactory.getLogger(AlertStore.class);

    private static final String INSERT =
            "INSERT INTO alerts (rule_id, alert_time, alert_keys, metrics, received_at)"
                    + " VALUES (?, ?, ?, ?, ?)";

    private final Connections connections;

    public AlertStore(Connections connections, boolean replay) throws SQLException {
        this.connections = connections;
        if (replay) {
            try (Connection connection = connections.get();
                    Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM alerts");
            }
        }
    }

    /**
     * Keep one alert, an alert which cannot be stored is logged and dropped, the live feed still
     * has it
     *
     * @param alert the json the rule engine wrote
     */
    public void insert(JsonNode alert) {
        try (Connection connection = connections.get();
                PreparedStatement statement = connection.prepareStatement(INSERT)) {
            JsonNode ruleId = alert.path("rule_id");
            statement.setObject(1, ruleId.isNumber() ? ruleId.asInt() : null);
            statement.setString(2, alert.path("alert_time").asText(null));
            statement.setString(3, Json.write(alert.path("keys")));
            statement.setString(4, Json.write(alert.path("metrics")));
            // ! kept as text, a jdbc timestamp would reach the json as epoch milliseconds
            statement.setString(5, Instant.now().toString());
            statement.executeUpdate();
        } catch (SQLException e) {
            LOG.warn("Cannot store the alert {}", alert, e);
        }
    }
}
