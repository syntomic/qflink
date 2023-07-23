package cn.syntomic.qflink.rule.functions.aggregation.accumulators;

import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class MinAccumulator implements QAccumulator {

    private double min = Double.POSITIVE_INFINITY;

    @Override
    public QAccumulator add(Object value) {
        min = Math.min(min, Double.valueOf(value.toString()));
        return this;
    }

    @Override
    public double getResult() {
        return min;
    }
}
