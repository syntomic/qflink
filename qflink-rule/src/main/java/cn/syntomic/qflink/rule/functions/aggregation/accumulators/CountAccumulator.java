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
    public double getResult() {
        return count;
    }
}
