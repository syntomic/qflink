package cn.syntomic.qflink.common.connectors.sink.qfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Appends the serialized records to a file, one line per record. */
public class QFileSinkWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(QFileSinkWriter.class);

    private static final byte[] NEW_LINE = "\n".getBytes(StandardCharsets.UTF_8);

    private final SerializationSchema<T> serializer;
    private final FileChannel channel;

    public QFileSinkWriter(Path path, boolean truncate, SerializationSchema<T> serializer)
            throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        this.serializer = serializer;
        // ! APPEND cannot be combined with TRUNCATE_EXISTING, empty the file once it is open
        this.channel =
                FileChannel.open(
                        path,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND);
        if (truncate) {
            channel.truncate(0);
        }

        LOG.info("QFile Sink: write to {}", path);
    }

    @Override
    public void write(T element, Context context) throws IOException {
        byte[] serialized = serializer.serialize(element);
        if (serialized == null) {
            return;
        }

        // ! one append per record, the subtasks share the file and only whole lines may interleave
        ByteBuffer line = ByteBuffer.allocate(serialized.length + NEW_LINE.length);
        line.put(serialized).put(NEW_LINE).flip();
        while (line.hasRemaining()) {
            channel.write(line);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        channel.force(false);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
