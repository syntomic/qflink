package cn.syntomic.qflink.rule.functions.windowing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.common.utils.TimeUtil;
import cn.syntomic.qflink.rule.entity.WindowRule;
import cn.syntomic.qflink.rule.entity.WindowRule.Trigger;
import cn.syntomic.qflink.rule.entity.WindowRule.WindowType;

public class DynamicWindowAssignerTest {

    static DynamicWindowAssigner windowAssigner = new DynamicWindowAssigner();

    @Test
    void testCumulate() {
        long timestamp = TimeUtil.stringToLong("2022-11-11 11:11:11");

        long size = 24 * 60 * 60 * 1000;
        long step = 60000;
        long offset = TimeUtil.stringToLong("2022-11-11 05:00:00");
        WindowRule rule = new WindowRule(WindowType.CUMULATE, Trigger.BATCH, size, offset, step);

        List<TimeWindow> windows = windowAssigner.assignWindows(timestamp, rule);

        assertEquals("2022-11-11 05:00:00", TimeUtil.longToString(windows.get(0).getStart()));
        assertEquals("2022-11-11 11:12:00", TimeUtil.longToString(windows.get(0).getEnd()));
    }

    @Test
    void testSlide() {
        long timestamp = TimeUtil.stringToLong("2022-11-11 11:11:11");

        long size = 24 * 60 * 60 * 1000;
        long step = 60000;
        long offset = TimeUtil.stringToLong("2022-11-11 05:00:00");
        WindowRule rule = new WindowRule(WindowType.SLIDE, Trigger.BATCH, size, offset, step);

        List<TimeWindow> windows = windowAssigner.assignWindows(timestamp, rule);

        assertEquals(
                "2022-11-10 11:12:00",
                TimeUtil.longToString(windows.get(windows.size() - 1).getStart()));
        assertEquals(
                "2022-11-11 11:12:00",
                TimeUtil.longToString(windows.get(windows.size() - 1).getEnd()));
    }

    @Test
    void testTumble() {
        long timestamp = TimeUtil.stringToLong("2022-11-11 11:11:11");

        long size = 24 * 60 * 60 * 1000;
        long step = 0;
        long offset = TimeUtil.stringToLong("2022-11-11 05:00:00");
        WindowRule rule = new WindowRule(WindowType.TUMBLE, Trigger.BATCH, size, offset, step);

        List<TimeWindow> windows = windowAssigner.assignWindows(timestamp, rule);

        assertEquals(1, windows.size());
        assertEquals("2022-11-11 05:00:00", TimeUtil.longToString(windows.get(0).getStart()));
        assertEquals("2022-11-12 05:00:00", TimeUtil.longToString(windows.get(0).getEnd()));
    }
}
