package cn.syntomic.qflink.sql.udf.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class CustomCollector implements Collector<Row> {
    private final List<Row> output = new ArrayList<>();

    @Override
    public void collect(Row record) {
        this.output.add(record);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    public List<Row> getOutputs() {
        return output;
    }
}
