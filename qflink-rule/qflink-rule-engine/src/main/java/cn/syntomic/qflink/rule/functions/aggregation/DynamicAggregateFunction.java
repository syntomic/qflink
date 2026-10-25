package cn.syntomic.qflink.rule.functions.aggregation;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.types.Row;

import cn.syntomic.qflink.rule.entity.Aggregate;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.AvgAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.CountAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.CountDistinctAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.MaxAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.MinAccumulator;
import cn.syntomic.qflink.rule.functions.aggregation.accumulators.SumAccumulator;
import cn.syntomic.qflink.rule.utils.AviatorUtil;

/**
 * Aggregate a slice by the aggregates of the rule.
 *
 * <p><b>The state of a slice is the accumulator itself, a window result is computed after the
 * slices it is composed of have been merged.</b>
 */
public class DynamicAggregateFunction
        implements AggregateFunction<Row, Map<String, QAccumulator>, Map<String, QAccumulator>> {

    @Override
    public Map<String, QAccumulator> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<String, QAccumulator> add(Row row, Map<String, QAccumulator> accumulator) {
        Object input;
        for (Aggregate agg : row.<Aggregate[]>getFieldAs(1)) {
            if (!accumulator.containsKey(agg.getName())) {
                accumulator.put(agg.getName(), initAccumulator(agg));
            }

            Row value = row.getFieldAs(0);
            if (AviatorUtil.filterData(value, agg.getFilter())) {
                // TODO support multi inputs
                input = value.getField(agg.getInputs()[0]);

                if (input != null) {
                    // only accumulate non null input
                    accumulator.put(agg.getName(), accumulator.get(agg.getName()).add(input));
                }
            }
        }

        return accumulator;
    }

    @Override
    public Map<String, QAccumulator> getResult(Map<String, QAccumulator> accumulator) {
        return accumulator;
    }

    @Override
    public Map<String, QAccumulator> merge(
            Map<String, QAccumulator> a, Map<String, QAccumulator> b) {
        if (b == null) {
            return a;
        }
        if (a == null) {
            return b;
        }

        for (Entry<String, QAccumulator> entry : b.entrySet()) {
            QAccumulator acc = a.get(entry.getKey());
            a.put(
                    entry.getKey(),
                    acc == null ? entry.getValue().copy() : acc.merge(entry.getValue()));
        }

        return a;
    }

    /**
     * Copy the accumulators of a slice, merging must not modify the state of a slice
     *
     * @param accumulator
     * @return
     */
    public static Map<String, QAccumulator> copy(Map<String, QAccumulator> accumulator) {
        Map<String, QAccumulator> copy = new HashMap<>(accumulator.size());
        for (Entry<String, QAccumulator> entry : accumulator.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().copy());
        }

        return copy;
    }

    /**
     * Compute the metrics of the merged accumulators
     *
     * @param accumulator
     * @return
     */
    public static Map<String, Double> toResult(Map<String, QAccumulator> accumulator) {
        Map<String, Double> result = new HashMap<>(accumulator.size());
        for (Entry<String, QAccumulator> entry : accumulator.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getResult());
        }

        return result;
    }

    private QAccumulator initAccumulator(Aggregate agg) {
        switch (agg.getMethod()) {
            case MAX:
                return new MaxAccumulator();
            case COUNT:
                return new CountAccumulator();
            case COUNT_DISTINCT:
                return new CountDistinctAccumulator();
            case SUM:
                return new SumAccumulator();
            case AVG:
                return new AvgAccumulator();
            case MIN:
                return new MinAccumulator();
            default:
                throw new IllegalArgumentException(
                        "Current don't support aggregation " + agg.getMethod());
        }
    }
}
