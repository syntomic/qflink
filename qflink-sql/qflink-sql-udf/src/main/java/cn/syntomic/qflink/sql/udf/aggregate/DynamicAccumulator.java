package cn.syntomic.qflink.sql.udf.aggregate;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.HintFlag;

public class DynamicAccumulator {

    @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
    public Map<String, Object> map = new HashMap<>();
}
