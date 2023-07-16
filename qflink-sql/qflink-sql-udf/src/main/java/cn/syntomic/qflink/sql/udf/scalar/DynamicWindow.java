package cn.syntomic.qflink.sql.udf.scalar;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;

import org.apache.flink.table.functions.ScalarFunction;

/** Get window start by window rule */
public class DynamicWindow extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter DEFAULT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final DateTimeFormatter DAY_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00");

    private static final DateTimeFormatter HOUR_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00");

    public String eval(String time, String windowRule) {

        LocalDateTime localDateTime = LocalDateTime.parse(time, DEFAULT_FORMAT);

        switch (windowRule.toUpperCase()) {
            case "WEEK":
                return DAY_FORMAT.format(
                        localDateTime.with(TemporalAdjusters.previous(DayOfWeek.MONDAY)));
            case "DAY":
                return DAY_FORMAT.format(localDateTime);
            case "HOUR":
                return HOUR_FORMAT.format(localDateTime);
            default:
                throw new IllegalArgumentException("Unsupported window " + windowRule);
        }
    }
}
