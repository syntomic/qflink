package cn.syntomic.qflink.sql.udf.ptfs;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

public class Greeting extends ProcessTableFunction<String> {
    public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
        collect("Hello " + input.getFieldAs("name") + "!");
    }
}
