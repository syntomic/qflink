package cn.syntomic.qflink.rule.functions.eventtime;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.types.Row;

import cn.syntomic.qflink.common.utils.TimeUtil;

public class LogRowTimestampAssigner implements SerializableTimestampAssigner<Row> {

    private final String fieldName;

    public LogRowTimestampAssigner(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public long extractTimestamp(Row element, long recordTimestamp) {
        return TimeUtil.stringToLong(element.getFieldAs(fieldName));
    }
}
