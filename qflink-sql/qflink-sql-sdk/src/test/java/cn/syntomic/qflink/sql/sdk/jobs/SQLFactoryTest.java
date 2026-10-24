package cn.syntomic.qflink.sql.sdk.jobs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SQLFactoryTest {

    @Test
    void testSplitStatements() {
        String sqls =
                "-- create the source\n"
                        + "CREATE TABLE src (a STRING) WITH ('connector' = 'qfile');\n"
                        + "INSERT INTO sink SELECT a FROM src;\n";

        assertArrayEquals(
                new String[] {
                    "CREATE TABLE src (a STRING) WITH ('connector' = 'qfile')",
                    "INSERT INTO sink SELECT a FROM src"
                },
                SQLFactory.splitStatements(sqls));
    }

    @Test
    void testKeepCommentMarkerInsideLiteral() {
        String sqls = "INSERT INTO sink SELECT '--not a comment' AS a FROM src;";

        String[] statements = SQLFactory.splitStatements(sqls);
        assertEquals(1, statements.length);
        assertEquals("INSERT INTO sink SELECT '--not a comment' AS a FROM src", statements[0]);
    }

    @Test
    void testKeepSemicolonInsideLiteral() {
        String sqls =
                "CREATE TABLE t (a STRING) WITH ('jaas' = 'user=\"u\" password=\"p;p\";');\n"
                        + "SELECT * FROM t;";

        assertArrayEquals(
                new String[] {
                    "CREATE TABLE t (a STRING) WITH ('jaas' = 'user=\"u\" password=\"p;p\";')",
                    "SELECT * FROM t"
                },
                SQLFactory.splitStatements(sqls));
    }

    @Test
    void testStripBlockComment() {
        String sqls = "/* header\n comment */\nSELECT 1;";

        assertArrayEquals(new String[] {"SELECT 1"}, SQLFactory.splitStatements(sqls));
    }

    @Test
    void testKeepHint() {
        String sqls =
                "SELECT * FROM t /*+ OPTIONS('scan.startup.mode' = 'earliest') */ /* note */;";

        assertArrayEquals(
                new String[] {"SELECT * FROM t /*+ OPTIONS('scan.startup.mode' = 'earliest') */"},
                SQLFactory.splitStatements(sqls));
    }
}
