package cn.syntomic.qflink.rule.codec.deser;

import static cn.syntomic.qflink.rule.configuration.RuleEngineConstants.RAW_FIELD;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

public class SimpleRowDeserializationSchema implements DeserializationSchema<Row> {

    @Override
    public Row deserialize(byte[] message) throws IOException {
        return Row.of(new String(message, StandardCharsets.UTF_8));
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(new String[] {RAW_FIELD}, Types.STRING);
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }
}
