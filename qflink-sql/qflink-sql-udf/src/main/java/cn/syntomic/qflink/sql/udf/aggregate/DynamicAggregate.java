package cn.syntomic.qflink.sql.udf.aggregate;

import org.apache.flink.table.functions.AggregateFunction;

public class DynamicAggregate extends AggregateFunction {

    @Override
    public Object getValue(Object accumulator) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getValue'");
    }

    @Override
    public Object createAccumulator() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createAccumulator'");
    }
}
