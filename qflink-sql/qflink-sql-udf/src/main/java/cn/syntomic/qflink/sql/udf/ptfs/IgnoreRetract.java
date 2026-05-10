package cn.syntomic.qflink.sql.udf.ptfs;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

@DataTypeHint("ROW<dim_a BIGINT, attr BIGINT, ts BIGINT>")
public class IgnoreRetract extends ProcessTableFunction<Row> implements ChangelogFunction {

    public void eval(
        @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.SUPPORT_UPDATES})
        Row input
    ) {

        if (input.getKind() == RowKind.INSERT || input.getKind() == RowKind.UPDATE_BEFORE) {
            collect(input);
        }
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
        return ChangelogMode.all();
    }
    
}
