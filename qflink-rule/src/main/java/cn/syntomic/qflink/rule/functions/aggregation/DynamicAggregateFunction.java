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

public class DynamicAggregateFunction
        implements AggregateFunction<Row, Map<String, QAccumulator>, Map<String, Double>> {

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
    public Map<String, Double> getResult(Map<String, QAccumulator> accumulator) {
        Map<String, Double> result = new HashMap<>();
        for (Entry<String, QAccumulator> entry : accumulator.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getResult());
        }

        return result;
    }

    @Override
    public Map<String, QAccumulator> merge(
            Map<String, QAccumulator> a, Map<String, QAccumulator> b) {
        throw new UnsupportedOperationException("Unimplemented method 'merge'");
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
