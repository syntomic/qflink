package cn.syntomic.qflink.rule.functions.windowing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import cn.syntomic.qflink.rule.entity.WindowRule;

public class DynamicWindowAssigner implements Serializable {

    public List<TimeWindow> assignWindows(long timestamp, WindowRule windowRule) {

        switch (windowRule.getType()) {
            case TUMBLE:
                return assignTumbleWindows(timestamp, windowRule.getSize(), windowRule.getOffset());
            case CUMULATE:
                return assignCumulateWindows(
                        timestamp,
                        windowRule.getSize(),
                        windowRule.getOffset(),
                        windowRule.getStep());
            case SLIDE:
                return assignSlideWindows(
                        timestamp,
                        windowRule.getSize(),
                        windowRule.getOffset(),
                        windowRule.getStep());
            default:
                throw new IllegalArgumentException(
                        "Not support window type: " + windowRule.getType().toString());
        }
    }

    /** tumble window */
    private List<TimeWindow> assignTumbleWindows(long timestamp, long windowSize, long offset) {
        long start = getWindowStartWithOffset(timestamp, offset, windowSize);
        return Collections.singletonList(new TimeWindow(start, start + windowSize));
    }

    /** accumulate window */
    private List<TimeWindow> assignCumulateWindows(
            long timestamp, long windowSize, long offset, long step) {
        List<TimeWindow> windows = new ArrayList<>();
        long start = getWindowStartWithOffset(timestamp, offset, windowSize);
        long lastEnd = start + windowSize;
        long firstEnd = getWindowStartWithOffset(timestamp, offset, step) + step;
        for (long end = firstEnd; end <= lastEnd; end += step) {
            windows.add(new TimeWindow(start, end));
        }
        return windows;
    }

    /** slide window */
    private List<TimeWindow> assignSlideWindows(
            long timestamp, long windowSize, long offset, long slide) {

        List<TimeWindow> windows = new ArrayList<>((int) (windowSize / slide));
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
        for (long start = lastStart; start > timestamp - windowSize; start -= slide) {
            windows.add(new TimeWindow(start, start + windowSize));
        }

        return windows;
    }

    public long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        long remainder = (timestamp - offset) % windowSize;
        // handle both positive and negative cases
        if (remainder < 0) {
            return timestamp - (remainder + windowSize);
        } else {
            return timestamp - remainder;
        }
    }
}
