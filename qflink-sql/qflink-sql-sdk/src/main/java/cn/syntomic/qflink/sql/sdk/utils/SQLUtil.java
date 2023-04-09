package cn.syntomic.qflink.sql.sdk.utils;

import java.util.LinkedHashMap;
import java.util.stream.IntStream;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

public class SQLUtil {

    /**
     * mask sql before show
     *
     * @param sql
     * @return
     */
    public static String maskSql(String sql) {
        // mask secret
        sql = sql.replaceAll("(password.*?=[ '\"]+)\\w+(['\"])", "$1*****$2");

        // show lineno
        String[] lines = sql.split("\r\n|\r|\n");
        String[] linesWithNum =
                IntStream.range(1, lines.length + 1)
                        .mapToObj(i -> i + "\t" + lines[i - 1])
                        .toArray(String[]::new);

        return System.getProperty("line.separator")
                + String.join(System.getProperty("line.separator"), linesWithNum);
    }

    /**
     * create row by row type info
     *
     * @param rowTypeInfo
     * @return
     */
    public static Row createRowByTypeInfo(RowTypeInfo rowTypeInfo) {
        LinkedHashMap<String, Integer> lMap = new LinkedHashMap<>(rowTypeInfo.getArity());
        for (int i = 0; i < rowTypeInfo.getArity(); i++) {
            lMap.put(rowTypeInfo.getFieldNames()[i], i);
        }

        // ! fieldByName not necessary
        return RowUtils.createRowWithNamedPositions(
                RowKind.INSERT, new Object[rowTypeInfo.getArity()], lMap);
    }

    private SQLUtil() {}
}
