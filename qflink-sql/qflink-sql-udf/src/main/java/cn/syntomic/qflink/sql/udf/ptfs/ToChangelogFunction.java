package cn.syntomic.qflink.sql.udf.ptfs;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

@DataTypeHint("ROW<flag STRING, sum INT>")
public class ToChangelogFunction extends ProcessTableFunction<Row> {
    public void eval(
            @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.SUPPORT_UPDATES})
                    Row input) {
        // Forwards the sum column and includes the row's kind as a string column.
        Row changelogRow = Row.of(input.getKind().toString(), input.getField("sum"));

        collect(changelogRow);
    }
}
