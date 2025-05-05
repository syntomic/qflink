package cn.syntomic.qflink.common.configuration;

import static cn.syntomic.qflink.common.connectors.ConnectorOptions.SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class QConfigurationTest {

    static QConfiguration conf;

    @BeforeAll
    static void setUp() {
        conf = new QConfiguration();
        conf.setString("log." + SOURCE.key(), "qfile");
        conf.set(SOURCE, "kafka");
    }

    @Test
    void testPrefix() throws Exception {
        assertEquals("qfile", conf.get("log", SOURCE));
        assertEquals("kafka", conf.get("rule", SOURCE));
    }
}
