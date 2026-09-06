package cn.syntomic.qflink.rule.functions.aggregation.accumulators;

import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class CountAccumulator implements QAccumulator {

    private long count = 0L;

    @Override
    public QAccumulator add(Object value) {
        count += 1;
        return this;
    }

    @Override
    public QAccumulator merge(QAccumulator other) {
        count += ((CountAccumulator) other).count;
        return this;
    }

    @Override
    public QAccumulator copy() {
        CountAccumulator acc = new CountAccumulator();
        acc.count = count;
        return acc;
    }

    @Override
    public double getResult() {
        return count;
    }
}
