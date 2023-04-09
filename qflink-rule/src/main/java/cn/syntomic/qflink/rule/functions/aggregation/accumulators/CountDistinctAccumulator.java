package cn.syntomic.qflink.rule.functions.aggregation.accumulators;

import java.util.HashSet;
import java.util.Set;

import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class CountDistinctAccumulator implements QAccumulator {

    private Set<String> distinctCnt = new HashSet<>();

    @Override
    public QAccumulator add(Object value) {
        distinctCnt.add(value.toString());
        return this;
    }

    @Override
    public double getResult() {
        return distinctCnt.size();
    }
}
