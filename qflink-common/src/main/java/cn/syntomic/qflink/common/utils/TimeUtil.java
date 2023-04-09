package cn.syntomic.qflink.common.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtil {

    public static final DateTimeFormatter DEFAULT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    /**
     * Convert time string to unix timestamp with default format
     *
     * @param time
     * @return
     */
    public static long stringToLong(String time) {
        return stringToLong(time, DEFAULT_FORMAT);
    }

    /**
     * Convert time string to unix timestamp
     *
     * @param time
     * @param ftf formatter with zone
     * @return
     */
    public static long stringToLong(String time, DateTimeFormatter ftf) {
        return ZonedDateTime.parse(time, ftf).toInstant().toEpochMilli();
    }

    /**
     * Convert unix timestamp to time string with default format
     *
     * @param timestamp
     * @return
     */
    public static String longToString(long timestamp) {
        return longToString(timestamp, DEFAULT_FORMAT);
    }

    /**
     * Convert unix timestamp to time string
     *
     * @param timestamp
     * @param ftf formatter with zone
     * @return
     */
    public static String longToString(long timestamp, DateTimeFormatter ftf) {
        return ftf.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ftf.getZone()));
    }

    private TimeUtil() {}
}
