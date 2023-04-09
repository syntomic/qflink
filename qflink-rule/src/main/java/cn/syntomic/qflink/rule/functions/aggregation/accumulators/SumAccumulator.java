package cn.syntomic.qflink.rule.functions.aggregation.accumulators;

import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class SumAccumulator implements QAccumulator {

    private double sum = Double.NaN;

    @Override
    public QAccumulator add(Object value) {
        if (Double.isNaN(sum)) {
            sum = (double) value;
        } else {
            sum += (double) value;
        }
        return this;
    }

    @Override
    public double getResult() {
        return sum;
    }
}
