package cn.syntomic.qflink.rule.functions.aggregation.accumulators;

import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class AvgAccumulator implements QAccumulator {
    private double sum = 0.0d;
    private long count = 0L;

    @Override
    public QAccumulator add(Object value) {
        count += 1;
        sum += Double.valueOf(value.toString());
        return this;
    }

    @Override
    public QAccumulator merge(QAccumulator other) {
        AvgAccumulator that = (AvgAccumulator) other;
        sum += that.sum;
        count += that.count;
        return this;
    }

    @Override
    public QAccumulator copy() {
        AvgAccumulator acc = new AvgAccumulator();
        acc.sum = sum;
        acc.count = count;
        return acc;
    }

    @Override
    public double getResult() {
        // ! doubles operation result is always a double
        return sum / count;
    }
}
