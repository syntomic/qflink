package cn.syntomic.qflink.sql.udf.ptfs;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("Row<dim_a BIGINT, metric BIGINT, ts BIGINT>"))
public class IgnoreRetractAggregation extends ProcessTableFunction<Row> implements ChangelogFunction {
    
    public void eval (
        @StateHint(type=@DataTypeHint("Row<dim_a BIGINT, metric BIGINT, ts BIGINT>")) Row state,
        @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.SUPPORT_UPDATES}) Row input
    ) throws Exception {
        
        switch (input.getKind()) {
            case UPDATE_BEFORE:
                // only update aggragation state, not emit record
                state.setField(1, state.<Long>getFieldAs(1) - input.<Long>getFieldAs(2)); 
                state.setField(2, input.<Long>getFieldAs(3));
                break;
            case UPDATE_AFTER:
            case INSERT:
                if (state.<Long>getFieldAs(0) == null) {
                    state.setField(0, input.getField(0));
                    state.setField(1, input.getField(2));
                    state.setField(2, input.getField(3));
                } else {
                    state.setField(1, state.<Long>getFieldAs(1) + input.<Long>getFieldAs(2));
                    state.setField(2, input.<Long>getFieldAs(3));
                }
                collect(state);
                break;
            case DELETE:
                // TODO
                break;
        }
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
        return ChangelogMode.all();
    }
}
