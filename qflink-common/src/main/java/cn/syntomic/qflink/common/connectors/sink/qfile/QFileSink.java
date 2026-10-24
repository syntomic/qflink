package cn.syntomic.qflink.common.connectors.sink.qfile;

import static cn.syntomic.qflink.common.connectors.sink.qfile.QFileSinkOptions.PATH;
import static cn.syntomic.qflink.common.connectors.sink.qfile.QFileSinkOptions.TRUNCATE;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import cn.syntomic.qflink.common.configuration.QConfiguration;
import cn.syntomic.qflink.common.utils.FileUtil;

/**
 * A mock sink which appends one line per record to a file, the counterpart of the qfile source.
 *
 * <p>Selected with {@code sink=qfile}, it lets a local run be observed by anything which tails a
 * file, e.g. the web console. <b>Every record is flushed, it is a development sink, not a
 * filesystem connector.</b>
 *
 * @param <T> The type of records consumed by this sink
 */
public class QFileSink<T> implements Sink<T> {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_DIR = "sinks";

    private final String path;
    private final boolean truncate;
    private final SerializationSchema<T> serializer;

    private QFileSink(String path, boolean truncate, SerializationSchema<T> serializer) {
        this.path = path;
        this.truncate = truncate;
        this.serializer = serializer;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        try {
            serializer.open(context.asSerializationSchemaInitializationContext());
        } catch (Exception e) {
            throw new IOException("Failed to initialize serializer", e);
        }

        // ! only one subtask may empty the shared file, the others would drop what it just wrote
        boolean truncateHere = truncate && context.getTaskInfo().getIndexOfThisSubtask() == 0;
        return new QFileSinkWriter<>(Path.of(path), truncateHere, serializer);
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /**
     * Create QFileSink from configuration
     *
     * @param conf Configuration
     * @param serializer Serialization schema
     * @param sinkName Sink name in configuration
     * @param <T> Record type
     * @return QFileSink instance
     */
    public static <T> QFileSink<T> of(
            QConfiguration conf, SerializationSchema<T> serializer, String sinkName) {

        return new QFileSink<>(
                FileUtil.getRelPath(DEFAULT_DIR, conf.get(sinkName, PATH)).toString(),
                conf.get(sinkName, TRUNCATE),
                serializer);
    }

    /**
     * Create QFileSink with explicit parameters
     *
     * @param path File path
     * @param truncate Whether to empty the file when the job starts
     * @param serializer Serialization schema
     * @param <T> Record type
     * @return QFileSink instance
     */
    public static <T> QFileSink<T> of(
            String path, boolean truncate, SerializationSchema<T> serializer) {

        return new QFileSink<>(
                FileUtil.getRelPath(DEFAULT_DIR, path).toString(), truncate, serializer);
    }
}
