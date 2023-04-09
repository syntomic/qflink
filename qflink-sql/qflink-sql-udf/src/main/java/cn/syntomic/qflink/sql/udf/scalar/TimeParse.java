package cn.syntomic.qflink.sql.udf.scalar;

import java.time.Instant;
import java.util.Date;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeParse extends ScalarFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TimeParse.class);

    public @DataTypeHint("TIMESTAMP_LTZ(3)") Instant eval(String time, String... parsePatterns) {
        try {
            Date date = DateUtils.parseDate(time, parsePatterns);
            return date.toInstant();
        } catch (Exception e) {
            LOG.error("Parse time error", e);
            return null;
        }
    }
}
