package cn.syntomic.qflink.rule.utils;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.flink.types.Row;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import org.apache.commons.lang3.StringUtils;

import cn.syntomic.qflink.common.entity.Field;
import cn.syntomic.qflink.rule.entity.Eval;

public class AviatorUtil {

    /** Init AviatorScript */
    public static void openAviator() {
        // add functions
    }

    /**
     * Determine if data is needed according to the filter
     *
     * @param data
     * @param filter
     * @return
     */
    public static boolean filterData(Row data, Eval filter) {
        if (filter != null && StringUtils.isNotBlank(filter.getExpr())) {
            Expression expression = AviatorEvaluator.compile(filter.getExpr(), true);

            return (boolean)
                    expression.execute(
                            expression.newEnv(
                                    Stream.of(filter.getParams())
                                            .flatMap(
                                                    param -> Stream.of(param, data.getField(param)))
                                            .toArray(Object[]::new)));
        } else {
            return true;
        }
    }

    /**
     * Combine fields expr together and eval later
     *
     * @param fields
     * @param row
     * @return
     */
    @SuppressWarnings("null")
    public static Row evalMapping(Row row, Field[] fields) {

        Row result = Row.withNames();

        // composite as unify expression
        StringBuilder sb = new StringBuilder();
        sb.append("tuple(");
        for (int i = 0; i < fields.length; i++) {
            sb.append(fields[i].getExpr());
            if (i == fields.length - 1) {
                sb.append(")");
            } else {
                sb.append(",");
            }
        }

        Expression expression = AviatorEvaluator.compile(sb.toString());
        Object[] values = (Object[]) expression.execute(RowUtil.toMap(row));
        for (int j = 0; j < values.length; j++) {
            result.setField(
                    fields[j].getName(), values[j] == null ? fields[j].getDefaulz() : values[j]);
        }

        // add original row
        for (String name : row.getFieldNames(true)) {
            result.setField(name, row.getField(name));
        }
        return result;
    }

    /**
     * Determine if data is alerted according to the filter
     *
     * @param data
     * @param threshold
     * @return
     */
    public static boolean alert(Map<String, Double> data, Eval threshold) {
        if (threshold != null && StringUtils.isNotBlank(threshold.getExpr())) {
            Expression expression = AviatorEvaluator.compile(threshold.getExpr(), true);

            return (boolean)
                    expression.execute(
                            expression.newEnv(
                                    Stream.of(threshold.getParams())
                                            .flatMap(param -> Stream.of(param, data.get(param)))
                                            .toArray(Object[]::new)));
        } else {
            return false;
        }
    }

    private AviatorUtil() {}
}
