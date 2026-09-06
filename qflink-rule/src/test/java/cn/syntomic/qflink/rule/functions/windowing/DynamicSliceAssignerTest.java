package cn.syntomic.qflink.rule.functions.windowing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.common.utils.TimeUtil;
import cn.syntomic.qflink.rule.entity.WindowRule;
import cn.syntomic.qflink.rule.entity.WindowRule.Trigger;
import cn.syntomic.qflink.rule.entity.WindowRule.WindowType;

public class DynamicSliceAssignerTest {

    static final DynamicSliceAssigner assigner = new DynamicSliceAssigner();

    static final long DAY = 24 * 60 * 60 * 1000L;
    static final long MINUTE = 60 * 1000L;

    static final long TIMESTAMP = TimeUtil.stringToLong("2022-11-11 11:11:11");
    static final long OFFSET = TimeUtil.stringToLong("2022-11-11 05:00:00");

    @Test
    void testTumble() {
        WindowRule rule = new WindowRule(WindowType.TUMBLE, Trigger.BATCH, DAY, OFFSET, 0);

        long sliceEnd = assigner.assignSliceEnd(TIMESTAMP, rule);

        // a tumble slice is the window itself
        assertEquals("2022-11-12 05:00:00", TimeUtil.longToString(sliceEnd));
        assertEquals(
                "2022-11-11 05:00:00", TimeUtil.longToString(assigner.windowStart(sliceEnd, rule)));
        assertEquals(sliceEnd, assigner.lastWindowEnd(sliceEnd, rule));
        assertEquals(List.of(sliceEnd), assigner.slicesOf(sliceEnd, rule));
        assertFalse(assigner.isShared(rule));
        assertFalse(assigner.nextWindowEnd(sliceEnd, rule).isPresent());
    }

    @Test
    void testCumulate() {
        WindowRule rule = new WindowRule(WindowType.CUMULATE, Trigger.BATCH, DAY, OFFSET, MINUTE);

        long sliceEnd = assigner.assignSliceEnd(TIMESTAMP, rule);

        // the element only lands in the step it belongs to, not in every pending window
        assertEquals("2022-11-11 11:12:00", TimeUtil.longToString(sliceEnd));
        assertEquals(MINUTE, assigner.sliceSize(rule));
        assertEquals(
                "2022-11-11 05:00:00", TimeUtil.longToString(assigner.windowStart(sliceEnd, rule)));
        assertEquals(
                "2022-11-12 05:00:00",
                TimeUtil.longToString(assigner.lastWindowEnd(sliceEnd, rule)));

        // the window of that step is made of every step since the window start
        List<Long> slices = assigner.slicesOf(sliceEnd, rule);
        assertEquals(6 * 60 + 12, slices.size());
        assertEquals("2022-11-11 05:01:00", TimeUtil.longToString(slices.get(0)));
        assertEquals(sliceEnd, slices.get(slices.size() - 1));

        // the ended slices are merged into the first slice of the window
        assertEquals(slices.get(0), assigner.mergeTarget(sliceEnd, rule).getAsLong());

        // the following steps are fired one after the other, until the window is complete
        assertEquals(sliceEnd + MINUTE, assigner.nextWindowEnd(sliceEnd, rule).getAsLong());
        long lastEnd = assigner.lastWindowEnd(sliceEnd, rule);
        assertFalse(assigner.nextWindowEnd(lastEnd, rule).isPresent());
    }

    @Test
    void testSlide() {
        WindowRule rule =
                new WindowRule(WindowType.SLIDE, Trigger.BATCH, 4 * MINUTE, OFFSET, MINUTE);

        long sliceEnd = assigner.assignSliceEnd(TIMESTAMP, rule);

        assertEquals("2022-11-11 11:12:00", TimeUtil.longToString(sliceEnd));
        assertEquals(MINUTE, assigner.sliceSize(rule));

        // the slice is shared by the 4 windows it belongs to, it is stored once
        assertEquals(
                "2022-11-11 11:15:00",
                TimeUtil.longToString(assigner.lastWindowEnd(sliceEnd, rule)));

        List<String> slices =
                assigner.slicesOf(sliceEnd, rule).stream()
                        .map(TimeUtil::longToString)
                        .collect(Collectors.toList());
        assertEquals(
                List.of(
                        "2022-11-11 11:09:00",
                        "2022-11-11 11:10:00",
                        "2022-11-11 11:11:00",
                        "2022-11-11 11:12:00"),
                slices);

        assertTrue(assigner.isShared(rule));
        assertFalse(assigner.mergeTarget(sliceEnd, rule).isPresent());
        assertEquals(sliceEnd + MINUTE, assigner.nextWindowEnd(sliceEnd, rule).getAsLong());
    }

    @Test
    void testNegativeTimestamp() {
        WindowRule rule = new WindowRule(WindowType.TUMBLE, Trigger.BATCH, MINUTE, 0, 0);

        assertEquals(0L, assigner.assignSliceEnd(-MINUTE, rule));
        assertEquals(0L, assigner.assignSliceEnd(-1, rule));
        assertEquals(MINUTE, assigner.assignSliceEnd(0, rule));
    }

    @Test
    void testValidate() {
        // a window is built from its slices, so it must be an integral multiple of the step
        WindowRule notSliceable =
                new WindowRule(WindowType.SLIDE, Trigger.BATCH, 10 * MINUTE, 0, 4 * MINUTE);
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> assigner.validate(notSliceable));
        assertTrue(error.getMessage().contains("integral multiple"), error.getMessage());

        assigner.validate(new WindowRule(WindowType.SLIDE, Trigger.BATCH, 10 * MINUTE, 0, MINUTE));
        assigner.validate(new WindowRule(WindowType.TUMBLE, Trigger.BATCH, MINUTE, 0, 0));
    }
}
