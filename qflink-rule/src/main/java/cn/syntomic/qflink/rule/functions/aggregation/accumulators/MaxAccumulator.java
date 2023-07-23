package cn.syntomic.qflink.rule.functions.aggregation.accumulators;

import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class MaxAccumulator implements QAccumulator {

    private double max = Double.NEGATIVE_INFINITY;

    @Override
    public QAccumulator add(Object value) {
        max = Math.max(max, Double.valueOf(value.toString()));
        return this;
    }

    @Override
    public double getResult() {
        return max;
    }
}
