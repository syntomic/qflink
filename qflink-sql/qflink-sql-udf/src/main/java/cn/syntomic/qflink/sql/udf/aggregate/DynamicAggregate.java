package cn.syntomic.qflink.sql.udf.aggregate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Aggregate by aggregate rule */
public class DynamicAggregate extends AggregateFunction<Map<String, Double>, DynamicAccumulator> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(DynamicAggregate.class);

    @Override
    public DynamicAccumulator createAccumulator() {
        return new DynamicAccumulator();
    }

    @SuppressWarnings("unchecked")
    public void accumulate(
            DynamicAccumulator acc,
            @DataTypeHint(inputGroup = InputGroup.ANY) Object data,
            @DataTypeHint("ROW<name STRING, input STRING, method STRING>") Row agg) {

        try {
            String name = agg.getFieldAs("name");
            switch (agg.<String>getFieldAs("method").toUpperCase()) {
                case "COUNT":
                    if (!acc.map.containsKey(name)) {
                        acc.map.put(name, 1L);
                    } else {
                        acc.map.put(name, (Long) acc.map.get(name) + 1L);
                    }
                    break;
                case "COUNT_DISTINCT":
                    Map<String, Integer> distinctMap;
                    if (!acc.map.containsKey(name)) {
                        distinctMap = new HashMap<>();
                    } else {
                        distinctMap = (Map<String, Integer>) acc.map.get(name);
                    }

                    distinctMap.compute(
                            ((Row) data).getFieldAs(agg.getFieldAs("input")),
                            (key, value) -> {
                                if (value == null) {
                                    return 1;
                                } else {
                                    return value + 1;
                                }
                            });
                    acc.map.put(name, distinctMap);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported aggregate method " + agg.getFieldAs("method"));
            }
        } catch (Exception e) {
            log.error("Dynamic agg error with data {} and agg {}", data, agg, e);
        }
    }

    @SuppressWarnings("unchecked")
    public void retract(
            DynamicAccumulator acc,
            @DataTypeHint(inputGroup = InputGroup.ANY) Row data,
            @DataTypeHint("ROW<name STRING, input STRING, method STRING>") Row agg) {
        try {

            String name = agg.getFieldAs("name");
            switch (agg.<String>getFieldAs("method").toUpperCase()) {
                case "COUNT":
                    if (acc.map.containsKey(name)) {
                        acc.map.put(name, (Long) acc.map.get(name) - 1L);
                    }
                    break;
                case "COUNT_DISTINCT":
                    Map<String, Integer> distinctMap;
                    if (acc.map.containsKey(name)) {
                        distinctMap = (Map<String, Integer>) acc.map.get(name);

                        Object value = data.getField(agg.getFieldAs("field"));
                        if (distinctMap.containsKey(value)) {
                            Integer cnt = distinctMap.get(value) - 1;

                            if (cnt <= 0) {
                                distinctMap.remove(value);
                            }
                        }
                        acc.map.put(name, distinctMap);
                    }

                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported aggregate method " + agg.getFieldAs("method"));
            }
        } catch (Exception e) {
            log.error("Dynamic agg retract error with data {} and agg {}", data, agg, e);
        }
    }

    public void merge(DynamicAccumulator acc, Iterable<DynamicAccumulator> it) {
        throw new UnsupportedOperationException("Merge method is not implemented yet.");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Double> getValue(DynamicAccumulator acc) {
        try {
            Map<String, Double> result = new HashMap<>();

            Iterator<Map.Entry<String, Object>> entries = acc.map.entrySet().iterator();
            while (entries.hasNext()) {

                Map.Entry<String, Object> entry = entries.next();
                if (entry.getValue() instanceof Map) {
                    result.put(
                            entry.getKey(),
                            Double.valueOf(((Map<String, Integer>) entry.getValue()).size()));
                } else {
                    result.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));
                }
            }

            return result;
        } catch (Exception e) {
            log.error("Get accumulator {} result error", acc.map, e);
            return Collections.emptyMap();
        }
    }
}
