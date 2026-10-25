package cn.syntomic.qflink.rule.configuration;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import cn.syntomic.qflink.rule.entity.Rule;
import cn.syntomic.qflink.rule.functions.aggregation.QAccumulator;

public class RuleEngineConstants {

    public static final String RAW_FIELD = "raw";

    public static final String RULE_ID = "rule_id";
    public static final String KEY = "key";
    public static final String SLICE_END = "slice_end";
    public static final String ACCUMULATORS = "accumulators";
    public static final String EXTRAS = "extras";
    public static final String ETL_TABLE = "etl.table";
    public static final String KEY_SEPARATOR = "#";

    public enum LogFormat {
        JSON,
        AVRO,
        RAW
    }

    public enum WatermarkTiming {
        WITHOUT,
        SOURCE,
        AFTER_ETL
    }

    public enum ETLOutput {
        INHERIT,
        SPECIFIC,
        GENERIC
    }

    public static final MapStateDescriptor<Integer, Rule> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("rules-broadcast-state", Types.INT, Types.POJO(Rule.class));

    public static final TypeInformation<Row> ALERT_TYPE =
            Types.ROW_NAMED(
                    new String[] {"rule_id", "alert_time", "keys", "metrics", "extras"},
                    Types.INT,
                    Types.STRING,
                    Types.MAP(Types.STRING, Types.STRING),
                    Types.MAP(Types.STRING, Types.DOUBLE),
                    Types.MAP(Types.STRING, Types.STRING));

    /** The fields of a partial result, the row is in position based mode after the shuffle. */
    public static final int PARTIAL_SLICE_END = 2;

    public static final int PARTIAL_ACCUMULATORS = 3;
    public static final int PARTIAL_EXTRAS = 4;

    /** The partial result of a local aggregate, one row per key and slice. */
    public static final TypeInformation<Row> PARTIAL_TYPE =
            Types.ROW_NAMED(
                    new String[] {RULE_ID, KEY, SLICE_END, ACCUMULATORS, EXTRAS},
                    Types.INT,
                    Types.STRING,
                    Types.LONG,
                    Types.MAP(Types.STRING, Types.GENERIC(QAccumulator.class)),
                    Types.MAP(Types.STRING, Types.STRING));

    public static final TypeInformation<Row> ERROR_TYPE =
            Types.ROW_NAMED(
                    new String[] {"job_identifier", "data", "rule", "error"},
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING);

    public static final OutputTag<Row> ERROR_TAG =
            new OutputTag<Row>("runtime-error", ERROR_TYPE) {};

    private RuleEngineConstants() {}
}
