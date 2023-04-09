package cn.syntomic.qflink.rule.functions.aggregation.accumulators;

import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class AvgAccumulator implements QAccumulator {
    private double sum = 0.0d;
    private long count = 0L;

    @Override
    public QAccumulator add(Object value) {
        count += 1;
        sum += (double) value;
        return this;
    }

    @Override
    public double getResult() {
        // ! doubles operation result is always a double
        return sum / count;
    }
}
