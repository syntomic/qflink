package cn.syntomic.qflink.sql.udf.scalar;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicKey extends ScalarFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(DynamicKey.class);

    @SuppressWarnings("null")
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object data, String keyField) {
        try {
            return ((Row) data).getField(keyField) == null
                    ? null
                    : ((Row) data).getField(keyField).toString();
        } catch (Exception e) {
            log.error("Dynamic key error with data {} and key {}", data, keyField, e);
            return null;
        }
    }
}
