package cn.syntomic.qflink.sql.udf.scalar;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Execute expression by aviator script */
public class DynamicFilter extends ScalarFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(DynamicFilter.class);

    public boolean eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object data, String filter) {

        try {
            if (StringUtils.isBlank(filter)) {
                return true;
            } else {
                Expression expression = AviatorEvaluator.compile(filter, true);
                return (boolean) expression.execute(toMap((Row) data));
            }
        } catch (Exception e) {
            log.error("Dynamic filter error with data {} and eval {}", data, filter, e);
            return false;
        }
    }

    public boolean eval(
            @DataTypeHint("MAP<STRING, DOUBLE>") Map<String, Double> data, String filter) {

        try {
            if (StringUtils.isBlank(filter)) {
                return true;
            } else {
                Expression expression = AviatorEvaluator.compile(filter, true);
                return (boolean) expression.execute(new HashMap<>(data));
            }
        } catch (Exception e) {
            log.error("Dynamic filter error with data {} and eval {}", data, filter, e);
            return false;
        }
    }

    @SuppressWarnings("null")
    private Map<String, Object> toMap(Row row) {
        return row.getFieldNames(true).stream()
                .collect(Collectors.toMap(name -> name, row::getField));
    }
}
