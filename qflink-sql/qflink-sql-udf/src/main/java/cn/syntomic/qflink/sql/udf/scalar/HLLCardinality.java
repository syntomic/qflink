package cn.syntomic.qflink.sql.udf.scalar;

import org.apache.flink.table.functions.ScalarFunction;

import net.agkn.hll.HLL;

public class HLLCardinality extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public long eval(byte[] input) {
        final HLL hll = HLL.fromBytes(input);
        return hll.cardinality();
    }
}
