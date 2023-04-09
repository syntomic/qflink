package cn.syntomic.qflink.sql.udf.table;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QJsonTuple extends TableFunction<Row> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(QJsonTuple.class);

    @Override
    public void open(FunctionContext context) throws Exception {}

    public void eval(String data, String... jsonPaths) {}
}
