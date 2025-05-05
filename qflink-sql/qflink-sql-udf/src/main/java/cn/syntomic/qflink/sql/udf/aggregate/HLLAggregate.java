package cn.syntomic.qflink.sql.udf.aggregate;

import java.nio.charset.Charset;

import org.apache.flink.table.functions.AggregateFunction;

import org.apache.flink.shaded.guava32.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava32.com.google.common.hash.Hashing;

import net.agkn.hll.HLL;

public class HLLAggregate extends AggregateFunction<byte[], HLLBuffer> {

    private final HashFunction hf = Hashing.murmur3_128();

    @Override
    public byte[] getValue(HLLBuffer acc) {
        return acc.hll.toBytes();
    }

    @Override
    public HLLBuffer createAccumulator() {
        return new HLLBuffer();
    }

    public void accumulate(HLLBuffer acc, String input) {
        if (input != null) {
            acc.hll.addRaw(
                    hf.newHasher().putString(input, Charset.defaultCharset()).hash().asLong());
        }
    }

    public void accumulate(HLLBuffer acc, byte[] input) {
        if (input != null) {
            acc.hll.union(HLL.fromBytes(input));
        }
    }

    public void merge(HLLBuffer acc, Iterable<HLLBuffer> it) {
        for (HLLBuffer tmpBuffer : it) {
            acc.hll.union(tmpBuffer.hll);
        }
    }

    public void resetAccumulator(HLLBuffer acc) {
        acc.hll.clear();
    }
}
