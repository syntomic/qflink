package cn.syntomic.qflink.rule.functions.aggregation.accumulators;

import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class SumAccumulator implements QAccumulator {

    private double sum = Double.NaN;

    @Override
    public QAccumulator add(Object value) {
        if (Double.isNaN(sum)) {
            sum = Double.valueOf(value.toString());
        } else {
            sum += Double.valueOf(value.toString());
        }
        return this;
    }

    @Override
    public double getResult() {
        return sum;
    }
}
