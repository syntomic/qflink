package cn.syntomic.qflink.sql.udf.scalar;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;

import org.junit.jupiter.api.Test;

public class TimeParseTest {
    @Test
    void testEval() {
        TimeParse timeParse = new TimeParse();

        assertEquals(
                timeParse.eval("2023-05-25 09:30:00", "yyyy-MM-dd HH:mm:ss"),
                Instant.parse("2023-05-25T01:30:00Z"));
    }
}
