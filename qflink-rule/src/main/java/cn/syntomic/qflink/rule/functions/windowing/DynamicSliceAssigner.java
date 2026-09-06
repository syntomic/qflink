package cn.syntomic.qflink.rule.functions.windowing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Preconditions;

import cn.syntomic.qflink.rule.entity.WindowRule;

/**
 * Assign an element to a single slice, like the window table valued functions of Flink SQL.
 *
 * <p>A slice is the smallest non overlapping time interval a window can be split into: {@code size}
 * for TUMBLE and {@code step} for SLIDE and CUMULATE. An element belongs to exactly one slice, so
 * it is accumulated <b>once</b> whatever the number of windows it belongs to, and a window result
 * is computed by merging the slices it is composed of.
 *
 * <p>Because windows are built from slices, the window size must be an integral multiple of the
 * step, the same restriction the window table valued functions have.
 */
public class DynamicSliceAssigner implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Check the window rule can be sliced
     *
     * @param rule
     */
    public void validate(WindowRule rule) {
        Preconditions.checkArgument(rule != null, "The window of an aggregate rule must not null");
        Preconditions.checkArgument(
                rule.getSize() > 0, "The window size must be positive, but got %s", rule.getSize());

        if (rule.getType() != WindowRule.WindowType.TUMBLE) {
            Preconditions.checkArgument(
                    rule.getStep() > 0,
                    "The %s window step must be positive, but got %s",
                    rule.getType(),
                    rule.getStep());
            Preconditions.checkArgument(
                    rule.getSize() % rule.getStep() == 0,
                    "The %s window size %s must be an integral multiple of the step %s",
                    rule.getType(),
                    rule.getSize(),
                    rule.getStep());
        }
    }

    /** The interval of a slice: the size for TUMBLE, the step for SLIDE and CUMULATE. */
    public long sliceSize(WindowRule rule) {
        return rule.getType() == WindowRule.WindowType.TUMBLE ? rule.getSize() : rule.getStep();
    }

    /** Whether the slices are shared by several windows, only TUMBLE slices are not. */
    public boolean isShared(WindowRule rule) {
        return rule.getType() != WindowRule.WindowType.TUMBLE;
    }

    /**
     * Assign the element to its only slice
     *
     * @param timestamp element timestamp
     * @param rule
     * @return the end of the slice
     */
    public long assignSliceEnd(long timestamp, WindowRule rule) {
        long sliceSize = sliceSize(rule);
        return getWindowStartWithOffset(timestamp, rule.getOffset(), sliceSize) + sliceSize;
    }

    /** The slice as a namespace of the window state. */
    public TimeWindow sliceOf(long sliceEnd, WindowRule rule) {
        return new TimeWindow(sliceEnd - sliceSize(rule), sliceEnd);
    }

    /** The window as a namespace of the window timers. */
    public TimeWindow windowOf(long windowEnd, WindowRule rule) {
        return new TimeWindow(windowStart(windowEnd, rule), windowEnd);
    }

    /** The start of the window which ends at the given time. */
    public long windowStart(long windowEnd, WindowRule rule) {
        switch (rule.getType()) {
            case TUMBLE:
            case SLIDE:
                return windowEnd - rule.getSize();
            case CUMULATE:
                // the cumulate window restarts every size, the end is the step boundary
                return getWindowStartWithOffset(windowEnd - 1, rule.getOffset(), rule.getSize());
            default:
                throw new IllegalArgumentException(
                        "Not support window type: " + rule.getType().toString());
        }
    }

    /**
     * The end of the last window the slice belongs to, the slice can be dropped afterwards
     *
     * @param sliceEnd
     * @param rule
     * @return
     */
    public long lastWindowEnd(long sliceEnd, WindowRule rule) {
        switch (rule.getType()) {
            case TUMBLE:
                return sliceEnd;
            case SLIDE:
                return sliceEnd + rule.getSize() - rule.getStep();
            case CUMULATE:
                return windowStart(sliceEnd, rule) + rule.getSize();
            default:
                throw new IllegalArgumentException(
                        "Not support window type: " + rule.getType().toString());
        }
    }

    /**
     * The slices the window is composed of, in time order
     *
     * <p>For CUMULATE the first slice is the merge target, it holds everything which has already
     * been merged, see {@link #mergeTarget(long, WindowRule)}.
     *
     * @param windowEnd
     * @param rule
     * @return
     */
    public List<Long> slicesOf(long windowEnd, WindowRule rule) {
        long sliceSize = sliceSize(rule);
        long start = windowStart(windowEnd, rule);

        List<Long> slices = new ArrayList<>((int) ((windowEnd - start) / sliceSize));
        for (long sliceEnd = start + sliceSize; sliceEnd <= windowEnd; sliceEnd += sliceSize) {
            slices.add(sliceEnd);
        }

        return slices;
    }

    /**
     * The slice the ended slices are merged into, only CUMULATE shares a state between its windows
     *
     * @param windowEnd
     * @param rule
     * @return the first slice of the window, empty when nothing is shared
     */
    public OptionalLong mergeTarget(long windowEnd, WindowRule rule) {
        if (rule.getType() != WindowRule.WindowType.CUMULATE) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(windowStart(windowEnd, rule) + rule.getStep());
    }

    /**
     * The end of the next window which has to be fired, as long as the state is not empty
     *
     * <p>The slices of a window only register a timer for the window they end, the following
     * windows of the same data are triggered one after the other.
     *
     * @param windowEnd
     * @param rule
     * @return
     */
    public OptionalLong nextWindowEnd(long windowEnd, WindowRule rule) {
        switch (rule.getType()) {
            case TUMBLE:
                return OptionalLong.empty();
            case SLIDE:
                return OptionalLong.of(windowEnd + rule.getStep());
            case CUMULATE:
                long next = windowEnd + rule.getStep();
                return next <= lastWindowEnd(windowEnd, rule)
                        ? OptionalLong.of(next)
                        : OptionalLong.empty();
            default:
                throw new IllegalArgumentException(
                        "Not support window type: " + rule.getType().toString());
        }
    }

    /**
     * Get the window start with offset, handle both positive and negative cases
     *
     * @param timestamp
     * @param offset
     * @param windowSize
     * @return
     */
    public long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        long remainder = (timestamp - offset) % windowSize;
        if (remainder < 0) {
            return timestamp - (remainder + windowSize);
        } else {
            return timestamp - remainder;
        }
    }
}
