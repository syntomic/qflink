package cn.syntomic.qflink.rule.functions.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.rule.entity.Aggregate;
import cn.syntomic.qflink.rule.entity.Aggregate.Method;

public class DynamicAggregateFunctionTest {

    static DynamicAggregateFunction aggFunction;
    static Aggregate[] aggs;

    @BeforeAll
    static void setUp() {
        aggFunction = new DynamicAggregateFunction();
        aggs =
                new Aggregate[] {
                    new Aggregate("sum", new String[] {"key"}, Method.SUM, null),
                    new Aggregate("max", new String[] {"key"}, Method.MAX, null),
                    new Aggregate("min", new String[] {"key"}, Method.MIN, null),
                    new Aggregate("avg", new String[] {"key"}, Method.AVG, null),
                    new Aggregate("count", new String[] {"key"}, Method.COUNT, null),
                    new Aggregate(
                            "count distinct", new String[] {"key"}, Method.COUNT_DISTINCT, null)
                };
    }

    @Test
    void testDynamicAggs() throws Exception {

        Map<String, QAccumulator> accs = new HashMap<>();

        for (Integer i = 0; i < 5; i++) {
            Row row = Row.withNames();
            row.setField("key", i);
            aggFunction.add(Row.of(row, aggs), accs);
        }

        assertEquals(10.0, DynamicAggregateFunction.toResult(accs).get("sum"));
        assertEquals(4.0, DynamicAggregateFunction.toResult(accs).get("max"));
        assertEquals(0.0, DynamicAggregateFunction.toResult(accs).get("min"));
        assertEquals(2.0, DynamicAggregateFunction.toResult(accs).get("avg"));
        assertEquals(5.0, DynamicAggregateFunction.toResult(accs).get("count"));
        assertEquals(5.0, DynamicAggregateFunction.toResult(accs).get("count distinct"));
    }

    @Test
    void testDistinctAggs() throws Exception {
        Map<String, QAccumulator> accs = new HashMap<>();

        for (Integer i = 0; i < 5; i++) {
            Row row = Row.withNames();
            row.setField("key", 0);
            aggFunction.add(Row.of(row, aggs), accs);
        }
        assertEquals(1.0, DynamicAggregateFunction.toResult(accs).get("count distinct"));
    }

    @Test
    void testMergeSlices() throws Exception {
        // every slice accumulates its own elements, a window merges them
        Map<String, QAccumulator> slice1 = new HashMap<>();
        Map<String, QAccumulator> slice2 = new HashMap<>();

        for (Integer i = 0; i < 3; i++) {
            Row row = Row.withNames();
            row.setField("key", i);
            aggFunction.add(Row.of(row, aggs), slice1);
        }
        for (Integer i = 2; i < 5; i++) {
            Row row = Row.withNames();
            row.setField("key", i);
            aggFunction.add(Row.of(row, aggs), slice2);
        }

        Map<String, Double> merged =
                DynamicAggregateFunction.toResult(
                        aggFunction.merge(DynamicAggregateFunction.copy(slice1), slice2));

        // same result as accumulating the 6 elements in one accumulator
        assertEquals(12.0, merged.get("sum"));
        assertEquals(4.0, merged.get("max"));
        assertEquals(0.0, merged.get("min"));
        assertEquals(2.0, merged.get("avg"));
        assertEquals(6.0, merged.get("count"));
        assertEquals(5.0, merged.get("count distinct"));

        // the slices are not modified by the merge
        assertEquals(3.0, DynamicAggregateFunction.toResult(slice1).get("count"));
        assertEquals(3.0, DynamicAggregateFunction.toResult(slice2).get("count"));
    }

    @Test
    void testNullAggs() throws Exception {
        Map<String, QAccumulator> accs = new HashMap<>();

        Row row = Row.withNames();
        row.setField("key", null);
        aggFunction.add(Row.of(row, aggs), accs);

        assertEquals(Double.NaN, DynamicAggregateFunction.toResult(accs).get("sum"));
        assertEquals(Double.NEGATIVE_INFINITY, DynamicAggregateFunction.toResult(accs).get("max"));
        assertEquals(Double.POSITIVE_INFINITY, DynamicAggregateFunction.toResult(accs).get("min"));
        assertEquals(Double.NaN, DynamicAggregateFunction.toResult(accs).get("avg"));
        assertEquals(0.0, DynamicAggregateFunction.toResult(accs).get("count"));
        assertEquals(0.0, DynamicAggregateFunction.toResult(accs).get("count distinct"));
    }
}
