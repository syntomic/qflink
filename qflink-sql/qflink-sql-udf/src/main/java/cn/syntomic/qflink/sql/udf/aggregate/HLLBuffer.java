package cn.syntomic.qflink.sql.udf.aggregate;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.HintFlag;

import net.agkn.hll.HLL;

public class HLLBuffer {

    @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
    // ! only need 1.2kb to estimate 1.6e+12 cardinality，error at +-0.02
    public HLL hll = new HLL(11, 5);
}
