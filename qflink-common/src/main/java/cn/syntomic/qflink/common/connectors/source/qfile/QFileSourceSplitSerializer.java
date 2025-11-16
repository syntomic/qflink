package cn.syntomic.qflink.common.connectors.source.qfile;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/** Serializer for {@link QFileSourceSplit}. */
public class QFileSourceSplitSerializer implements SimpleVersionedSerializer<QFileSourceSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(QFileSourceSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeUTF(split.splitId());
        out.writeUTF(split.getFilePath());
        out.writeLong(split.getStartOffset());
        out.writeLong(split.getEndOffset());
        return out.getCopyOfBuffer();
    }

    @SuppressWarnings("null")
    @Override
    public QFileSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }

        DataInputDeserializer in = new DataInputDeserializer(serialized);
        String splitId = in.readUTF();
        String filePath = in.readUTF();
        long startOffset = in.readLong();
        long endOffset = in.readLong();

        return new QFileSourceSplit(splitId, filePath, startOffset, endOffset);
    }
}
