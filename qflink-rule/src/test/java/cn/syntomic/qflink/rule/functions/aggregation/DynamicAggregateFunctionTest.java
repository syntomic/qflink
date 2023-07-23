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

        assertEquals(10.0, aggFunction.getResult(accs).get("sum"));
        assertEquals(4.0, aggFunction.getResult(accs).get("max"));
        assertEquals(0.0, aggFunction.getResult(accs).get("min"));
        assertEquals(2.0, aggFunction.getResult(accs).get("avg"));
        assertEquals(5.0, aggFunction.getResult(accs).get("count"));
        assertEquals(5.0, aggFunction.getResult(accs).get("count distinct"));
    }

    @Test
    void testDistinctAggs() throws Exception {
        Map<String, QAccumulator> accs = new HashMap<>();

        for (Integer i = 0; i < 5; i++) {
            Row row = Row.withNames();
            row.setField("key", 0);
            aggFunction.add(Row.of(row, aggs), accs);
        }
        assertEquals(1.0, aggFunction.getResult(accs).get("count distinct"));
    }

    @Test
    void testNullAggs() throws Exception {
        Map<String, QAccumulator> accs = new HashMap<>();

        Row row = Row.withNames();
        row.setField("key", null);
        aggFunction.add(Row.of(row, aggs), accs);

        assertEquals(Double.NaN, aggFunction.getResult(accs).get("sum"));
        assertEquals(Double.NEGATIVE_INFINITY, aggFunction.getResult(accs).get("max"));
        assertEquals(Double.POSITIVE_INFINITY, aggFunction.getResult(accs).get("min"));
        assertEquals(Double.NaN, aggFunction.getResult(accs).get("avg"));
        assertEquals(0.0, aggFunction.getResult(accs).get("count"));
        assertEquals(0.0, aggFunction.getResult(accs).get("count distinct"));
    }
}
