package cn.syntomic.qflink.sql.sdk.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class SQLUtilTest {

    @Test
    void testMaskConnectorOption() {
        String masked = SQLUtil.maskSql("CREATE TABLE t WITH ('password' = 'p@ss-w0rd!')");

        assertFalse(masked.contains("p@ss-w0rd!"));
        assertTrue(masked.contains("'password' = '*****'"));
    }

    @Test
    void testMaskNestedSecret() {
        String masked =
                SQLUtil.maskSql(
                        "CREATE TABLE t WITH ('properties.sasl.jaas.config' = 'org.Module required"
                                + " username=\"u\" password=\"s3cr3t!\";')");

        assertFalse(masked.contains("s3cr3t!"));
    }

    @Test
    void testMaskOtherSecretKeys() {
        assertFalse(SQLUtil.maskSql("SET 'access-key' = 'AKIA123/456+789'").contains("AKIA123"));
        assertFalse(SQLUtil.maskSql("SET 'fs.s3a.secret' = 'abc.def'").contains("abc.def"));
        assertFalse(SQLUtil.maskSql("SET 'token' = 'ya29.a0-x'").contains("ya29.a0-x"));
    }

    @Test
    void testKeepNormalOption() {
        String masked = SQLUtil.maskSql("CREATE TABLE t WITH ('topic' = 'my-topic')");

        assertTrue(masked.contains("'topic' = 'my-topic'"));
    }
}
