package cn.syntomic.qflink.rule.codec.ser;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.ETL_TABLE;

import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.types.Row;

public class DynamicTopicSelector implements TopicSelector<Row> {

    @Override
    public String apply(Row t) {
        // ! must specific this field
        return t.getFieldAs(ETL_TABLE);
    }
}
