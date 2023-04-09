package cn.syntomic.qflink.rule.utils;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.types.Row;

public class RowUtil {

    /**
     * Add extras fields to row
     *
     * @param row
     * @param names field name
     * @param values field value
     * @return
     */
    public static Row join(Row row, String[] names, Object... values) {
        if (row.getFieldNames(false) == null) {
            return Row.join(row, Row.of(values));
        } else {
            for (int i = 0; i < names.length; i++) {
                row.setField(names[i], values[i]);
            }
            return row;
        }
    }

    /**
     * Convert named row to map
     *
     * @param row
     * @return
     */
    @SuppressWarnings("null")
    public static Map<String, Object> toMap(Row row) {
        return row.getFieldNames(true).stream()
                .collect(Collectors.toMap(name -> name, row::getField));
    }

    /**
     * Convert named row to string map
     *
     * @param row
     * @return
     */
    @SuppressWarnings("null")
    public static Map<String, String> toStringMap(Row row) {
        return row.getFieldNames(true).stream()
                .collect(Collectors.toMap(name -> name, value -> row.getField(value).toString()));
    }

    private RowUtil() {}
}
