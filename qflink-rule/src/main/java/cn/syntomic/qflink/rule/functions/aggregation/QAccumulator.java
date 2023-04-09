package cn.syntomic.qflink.rule.functions.aggregation;

public interface QAccumulator {

    /**
     * Accumulate value of given type
     *
     * @param value
     * @return
     */
    QAccumulator add(Object value);

    /**
     * Get accumulate result
     *
     * @return
     */
    double getResult();
}
