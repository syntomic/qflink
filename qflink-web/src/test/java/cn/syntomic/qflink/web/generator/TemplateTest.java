package cn.syntomic.qflink.web.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.web.configuration.Durations;

class TemplateTest {

    private final Template template = new Template();

    @Test
    void aLineWithoutAPlaceholderIsLeftAlone() {
        assertEquals("2023-04-09 15:40:05 stdout", template.expand("2023-04-09 15:40:05 stdout"));
    }

    @Test
    void theTimeIsTheFormatTheLogSourceParses() {
        String expanded = template.expand("${time} stdout");

        assertTrue(
                Pattern.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} stdout", expanded),
                expanded);
    }

    @Test
    void theSequenceCounts() {
        assertEquals("1 2", template.expand("${seq} ${seq}"));
        assertEquals("3", template.expand("${seq}"));
    }

    @Test
    void randomValuesStayWithinTheirBounds() {
        for (int i = 0; i < 200; i++) {
            int value = Integer.parseInt(template.expand("${int(1,3)}"));
            assertTrue(value >= 1 && value <= 3, "out of bounds: " + value);
            assertTrue(template.expand("${choice(a|b)}").matches("[ab]"));
        }
    }

    @Test
    void theWholeDemoLineExpands() {
        String expanded =
                template.expand(
                        "${time} stdout {\"key1\": ${int(1,10)},\"key2\":\"val${int(1,5)}\"}");

        assertTrue(
                Pattern.matches(
                        "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} stdout \\{\"key1\": \\d+,\"key2\":\"val\\d\"\\}",
                        expanded),
                expanded);
    }

    @Test
    void anUnknownPlaceholderIsReported() {
        assertThrows(IllegalArgumentException.class, () -> template.expand("${nope}"));
    }

    @Test
    void flinkDurationsAreUnderstood() {
        assertEquals(2000L, Durations.toMillis("2 s", 0));
        assertEquals(500L, Durations.toMillis("500ms", 0));
        assertEquals(60_000L, Durations.toMillis("1 min", 0));
        assertEquals(42L, Durations.toMillis("42", 0));
        assertEquals(7L, Durations.toMillis("not a duration", 7));
        assertEquals(7L, Durations.toMillis(null, 7));
    }
}
