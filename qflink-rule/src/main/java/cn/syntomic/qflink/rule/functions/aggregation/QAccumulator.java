package cn.syntomic.qflink.rule.functions.aggregation;

import java.io.Serializable;

public interface QAccumulator extends Serializable {

    /**
     * Accumulate value of given type
     *
     * @param value
     * @return
     */
    QAccumulator add(Object value);

    /**
     * Merge another accumulator of the same aggregate into this one
     *
     * <p><b>A window is computed by merging the slices it is composed of, every accumulator must
     * support it.</b>
     *
     * @param other
     * @return
     */
    QAccumulator merge(QAccumulator other);

    /**
     * Copy the accumulator, a merge must not modify the accumulator of a slice
     *
     * @return
     */
    QAccumulator copy();

    /**
     * Get accumulate result
     *
     * @return
     */
    double getResult();
}
