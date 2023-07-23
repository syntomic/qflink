package cn.syntomic.qflink.rule.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import cn.syntomic.qflink.common.entity.Field;
import cn.syntomic.qflink.common.entity.Field.FieldType;

public class AviatorUtilTest {

    @BeforeAll
    static void setUp() {
        AviatorUtil.openAviator();
    }

    @Test
    void testEvalMapping() {

        Row row = Row.withNames();
        row.setField("field_1", "a");
        row.setField("field_2", "b");
        row.setField("field_3", "c");

        Field[] fields =
                new Field[] {
                    new Field("final_1", FieldType.INT, "string.length(field_1)", -1),
                    new Field("final_2", FieldType.STRING, "field_2 + field_3", "")
                };

        Row result2 = Row.withNames();
        result2.setField("field_1", "a");
        result2.setField("field_2", "b");
        result2.setField("field_3", "c");
        result2.setField("final_1", 1L);
        result2.setField("final_2", "bc");

        assertEquals(result2, AviatorUtil.evalMapping(row, fields));
    }
}
