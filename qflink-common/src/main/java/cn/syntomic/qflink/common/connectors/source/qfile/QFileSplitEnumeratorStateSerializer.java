package cn.syntomic.qflink.common.connectors.source.qfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/** Serializer for {@link QFileSplitEnumeratorState}. */
public class QFileSplitEnumeratorStateSerializer
        implements SimpleVersionedSerializer<QFileSplitEnumeratorState> {

    private static final int VERSION = 1;
    private final QFileSourceSplitSerializer splitSerializer = new QFileSourceSplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(QFileSplitEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);

        List<QFileSourceSplit> splits = state.getRemaingSplits();
        out.writeInt(splits.size());

        for (QFileSourceSplit split : splits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }

        return out.getCopyOfBuffer();
    }

    @SuppressWarnings("null")
    @Override
    public QFileSplitEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }

        DataInputDeserializer in = new DataInputDeserializer(serialized);
        int numSplits = in.readInt();
        List<QFileSourceSplit> splits = new ArrayList<>(numSplits);

        for (int i = 0; i < numSplits; i++) {
            int splitBytesLength = in.readInt();
            byte[] splitBytes = new byte[splitBytesLength];
            in.read(splitBytes);
            splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
        }

        return new QFileSplitEnumeratorState(splits);
    }
}
