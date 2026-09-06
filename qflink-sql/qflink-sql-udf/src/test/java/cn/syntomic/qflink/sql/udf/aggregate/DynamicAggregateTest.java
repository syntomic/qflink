package cn.syntomic.qflink.sql.udf.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import org.junit.jupiter.api.Test;

public class DynamicAggregateTest {

    private static final Row COUNT_AGG = agg("cnt", "val", "COUNT");
    private static final Row COUNT_DISTINCT_AGG = agg("uv", "val", "COUNT_DISTINCT");

    @Test
    void testCount() {
        DynamicAggregate function = new DynamicAggregate();
        DynamicAccumulator acc = function.createAccumulator();

        function.accumulate(acc, data("a"), COUNT_AGG);
        function.accumulate(acc, data("b"), COUNT_AGG);
        assertEquals(2.0, function.getValue(acc).get("cnt"));

        function.retract(acc, data("b"), COUNT_AGG);
        assertEquals(1.0, function.getValue(acc).get("cnt"));
    }

    @Test
    void testCountDistinctRetract() {
        DynamicAggregate function = new DynamicAggregate();
        DynamicAccumulator acc = function.createAccumulator();

        function.accumulate(acc, data("a"), COUNT_DISTINCT_AGG);
        function.accumulate(acc, data("a"), COUNT_DISTINCT_AGG);
        function.accumulate(acc, data("b"), COUNT_DISTINCT_AGG);
        assertEquals(2.0, function.getValue(acc).get("uv"));

        // 'a' was accumulated twice, it is still present after one retraction
        function.retract(acc, data("a"), COUNT_DISTINCT_AGG);
        assertEquals(2.0, function.getValue(acc).get("uv"));

        function.retract(acc, data("a"), COUNT_DISTINCT_AGG);
        assertEquals(1.0, function.getValue(acc).get("uv"));

        function.retract(acc, data("b"), COUNT_DISTINCT_AGG);
        assertEquals(0.0, function.getValue(acc).get("uv"));
    }

    private static Row data(String value) {
        LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>(1);
        positionByName.put("val", 0);
        return RowUtils.createRowWithNamedPositions(
                RowKind.INSERT, new Object[] {value}, positionByName);
    }

    private static Row agg(String name, String input, String method) {
        LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>(3);
        positionByName.put("name", 0);
        positionByName.put("input", 1);
        positionByName.put("method", 2);
        return RowUtils.createRowWithNamedPositions(
                RowKind.INSERT, new Object[] {name, input, method}, positionByName);
    }
}
