package cn.syntomic.qflink.common.connectors.source.qfile;

import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.INTERVAL;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.PATH;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.PAUSE;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.SCAN_TYPE;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import cn.syntomic.qflink.common.configuration.QConfiguration;
import cn.syntomic.qflink.common.utils.FileUtil;

/**
 * QFile Source implementation using the new Source API.
 *
 * <p>This source reads from a file based on the configured scan type: - once: Read file once in
 * order - rand: Read file randomly - cont: Continuously read file and monitor new lines
 *
 * @param <T> The type of records produced by this source
 */
public class QFileSource<T> implements Source<T, QFileSourceSplit, QFileSplitEnumeratorState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_DIR = "sources";

    private final String path;
    private final String scanType;
    private final long interval;
    private final long pause;
    private final DeserializationSchema<T> deserializer;

    private QFileSource(
            String path,
            String scanType,
            long interval,
            long pause,
            DeserializationSchema<T> deserializer) {
        this.path = path;
        this.scanType = scanModeWrapper(scanType);
        this.interval = interval;
        this.pause = pause;
        this.deserializer = deserializer;
    }

    /**
     * Compatible with kafka scan mode config
     *
     * @param scanMode
     * @return
     */
    private static String scanModeWrapper(String scanMode) {
        return "latest-offset".equals(scanMode) ? "cont" : scanMode;
    }

    @Override
    public Boundedness getBoundedness() {
        // "once" mode is bounded, others are unbounded
        return "once".equals(scanType) ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, QFileSourceSplit> createReader(SourceReaderContext readerContext) {
        return new QFileSourceReader<>(readerContext, deserializer, scanType, interval, pause);
    }

    @Override
    public SplitEnumerator<QFileSourceSplit, QFileSplitEnumeratorState> createEnumerator(
            SplitEnumeratorContext<QFileSourceSplit> enumContext) {
        return new QFileSplitEnumerator(enumContext, path, null);
    }

    @Override
    public SplitEnumerator<QFileSourceSplit, QFileSplitEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<QFileSourceSplit> enumContext,
            QFileSplitEnumeratorState checkpoint) {
        return new QFileSplitEnumerator(enumContext, path, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<QFileSourceSplit> getSplitSerializer() {
        return new QFileSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<QFileSplitEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new QFileSplitEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /**
     * Create QFileSource from configuration
     *
     * @param conf Configuration
     * @param deserializer Deserialization schema
     * @param sourceName Source name in configuration
     * @param <T> Record type
     * @return QFileSource instance
     */
    public static <T> QFileSource<T> of(
            QConfiguration conf, DeserializationSchema<T> deserializer, String sourceName) {

        return new QFileSource<>(
                FileUtil.getRelPath(DEFAULT_DIR, conf.get(sourceName, PATH)).toString(),
                conf.get(sourceName, SCAN_TYPE).toLowerCase(),
                conf.get(sourceName, INTERVAL).toMillis(),
                conf.get(sourceName, PAUSE).toMillis(),
                deserializer);
    }

    /**
     * Create QFileSource with explicit parameters
     *
     * @param path File path
     * @param scanType Scan type (once, rand, cont)
     * @param interval Interval between reads in milliseconds
     * @param pause Pause before starting in milliseconds
     * @param deserializer Deserialization schema
     * @param <T> Record type
     * @return QFileSource instance
     */
    public static <T> QFileSource<T> of(
            String path,
            String scanType,
            long interval,
            long pause,
            DeserializationSchema<T> deserializer) {

        return new QFileSource<>(
                FileUtil.getRelPath(DEFAULT_DIR, path).toString(),
                scanType,
                interval,
                pause,
                deserializer);
    }
}
