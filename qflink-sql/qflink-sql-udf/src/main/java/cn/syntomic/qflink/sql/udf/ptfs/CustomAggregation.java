package cn.syntomic.qflink.sql.udf.ptfs;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class CustomAggregation extends ProcessTableFunction<Row> implements ChangelogFunction {
    @Override
    public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
        // Tells the system that the PTF produces updates encoded as retractions
        return ChangelogMode.all();
    }

    public static class Accumulator {
        public Integer sum = 0;
    }

    public void eval(
            @StateHint Accumulator state,
            @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
        int score = input.getFieldAs("score");

        // A negative state indicates that the partition
        // key has been marked as invalid before
        if (state.sum == -1) {
            return;
        }

        // A negative score marks the entire aggregation result as invalid.
        if (score < 0) {
            // Send out a -D for the affected partition key and
            // mark the invalidation in state. All subsequent operations
            // and sinks will remove the aggregation result.
            collect(Row.ofKind(RowKind.DELETE, state.sum));
            state.sum = -1;
        } else {
            if (state.sum == 0) {
                // Emit +I for the first valid aggregation result.
                state.sum += score;
                collect(Row.ofKind(RowKind.INSERT, state.sum));
            } else {
                // Emit -U (with old aggregation result) and +U (with new aggregation result)
                // for encoding the update.
                collect(Row.ofKind(RowKind.UPDATE_BEFORE, state.sum));
                state.sum += score;
                collect(Row.ofKind(RowKind.UPDATE_AFTER, state.sum));
            }
        }
    }
}
