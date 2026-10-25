package cn.syntomic.qflink.rule.functions.windowing;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.flink.types.Row;

import cn.syntomic.qflink.rule.entity.Aggregate;
import cn.syntomic.qflink.rule.functions.aggregation.DynamicAggregateFunction;
import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;
import cn.syntomic.qflink.rule.utils.RowUtil;

/**
 * Buffer of the elements which have not been written to the window state yet, like the mini batch
 * of the window table valued functions.
 *
 * <p>The elements of a slice are accumulated in memory and the buffer is flushed as a whole, so a
 * slice is read and written <b>once per batch</b> instead of once per element. The buffer is not a
 * state, it is flushed before a watermark, before a checkpoint and when it is full.
 *
 * @param <K> the type of the keyed state key
 */
public class MiniBatchWindowBuffer<K> {

    private final DynamicAggregateFunction aggFunction;

    /** key -> slice end -> the elements of that slice, already aggregated. */
    private final Map<K, Map<Long, BufferedSlice>> buffer = new HashMap<>();

    private long count;

    public MiniBatchWindowBuffer(DynamicAggregateFunction aggFunction) {
        this.aggFunction = aggFunction;
    }

    /**
     * Accumulate the element in memory instead of writing it to the state
     *
     * @param key
     * @param sliceEnd
     * @param row
     * @param timestamp
     * @param aggregates
     */
    public void add(K key, long sliceEnd, Row row, long timestamp, Aggregate[] aggregates) {
        BufferedSlice slice =
                buffer.computeIfAbsent(key, k -> new HashMap<>())
                        .computeIfAbsent(sliceEnd, end -> new BufferedSlice());

        aggFunction.add(Row.of(row, aggregates), slice.accumulators);
        slice.lastRow = row;
        slice.extras = null;
        slice.lastTimestamp = timestamp;
        slice.count += 1;

        count += 1;
    }

    /**
     * Merge the partial accumulators of a local aggregate into the buffer
     *
     * @param key
     * @param sliceEnd
     * @param partial the accumulators of one slice, computed before the shuffle
     * @param extras the alert context of the partial result
     * @param timestamp
     */
    public void merge(
            K key,
            long sliceEnd,
            Map<String, QAccumulator> partial,
            Map<String, String> extras,
            long timestamp) {
        BufferedSlice slice =
                buffer.computeIfAbsent(key, k -> new HashMap<>())
                        .computeIfAbsent(sliceEnd, end -> new BufferedSlice());

        aggFunction.merge(slice.accumulators, partial);
        slice.extras = extras;
        slice.lastTimestamp = timestamp;
        slice.count += 1;

        count += 1;
    }

    /**
     * Drop the buffered elements of the keys matching the predicate, used when the state of a rule
     * is cleared
     *
     * @param filter
     */
    public void removeIf(Predicate<K> filter) {
        buffer.entrySet()
                .removeIf(
                        entry -> {
                            if (!filter.test(entry.getKey())) {
                                return false;
                            }

                            count -=
                                    entry.getValue().values().stream()
                                            .mapToLong(BufferedSlice::getCount)
                                            .sum();
                            return true;
                        });
    }

    /** The number of buffered elements. */
    public long count() {
        return count;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public Set<Entry<K, Map<Long, BufferedSlice>>> entries() {
        return buffer.entrySet();
    }

    public void clear() {
        buffer.clear();
        count = 0;
    }

    /** The buffered elements of one slice, aggregated in memory. */
    public static class BufferedSlice {

        private final Map<String, QAccumulator> accumulators = new HashMap<>();

        /** The last element of the slice, the context of an early fire alert. */
        private Row lastRow;

        /** The alert context of a partial result, the element itself did not cross the shuffle. */
        private Map<String, String> extras;

        private long lastTimestamp;
        private long count;

        public Map<String, QAccumulator> getAccumulators() {
            return accumulators;
        }

        public Row getLastRow() {
            return lastRow;
        }

        public Map<String, String> getExtras() {
            return extras == null ? RowUtil.toStringMap(lastRow) : extras;
        }

        public long getLastTimestamp() {
            return lastTimestamp;
        }

        public long getCount() {
            return count;
        }
    }
}
