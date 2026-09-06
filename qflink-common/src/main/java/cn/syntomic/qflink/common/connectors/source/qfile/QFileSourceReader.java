package cn.syntomic.qflink.common.connectors.source.qfile;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SourceReader for QFile that reads from file based on scan type.
 *
 * <p>The reader never blocks the mailbox thread: throttling(scan interval and initial pause) is
 * expressed by {@link #isAvailable()}, so checkpoint barriers are not delayed.
 */
public class QFileSourceReader<T> implements SourceReader<T, QFileSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(QFileSourceReader.class);

    private static final CompletableFuture<Void> AVAILABLE =
            CompletableFuture.completedFuture(null);

    /** Minimum wait before the file is checked again, avoids a busy loop when interval is 0. */
    private static final long IDLE_WAIT_MS = 100L;

    private final SourceReaderContext context;
    private final DeserializationSchema<T> deserializer;
    private final String scanType;
    private final long interval;
    private final long pause;

    private final OutputCollector<T> collector = new OutputCollector<>();

    private transient ScheduledExecutorService scheduler;

    private BufferedReader currentReader;
    private QFileSourceSplit currentSplit;

    /** Number of lines already consumed from the split, used to resume after a restore. */
    private long position;

    private boolean splitFinished;
    private boolean noMoreSplits;

    /** Wall clock time before which no record may be read. */
    private long nextReadTime;

    /** Completed as soon as a split is assigned or the enumerator signals there is none. */
    private CompletableFuture<Void> splitAssigned = new CompletableFuture<>();

    // For rand mode
    private List<String> fileLines;
    private Random random;

    public QFileSourceReader(
            SourceReaderContext context,
            DeserializationSchema<T> deserializer,
            String scanType,
            long interval,
            long pause) {
        this.context = context;
        this.deserializer = deserializer;
        this.scanType = scanType;
        this.interval = interval;
        this.pause = pause;

        if ("rand".equals(scanType)) {
            this.random = new Random();
        }
    }

    @Override
    public void start() {
        // Initialize deserializer
        try {
            deserializer.open(
                    new DeserializationSchema.InitializationContext() {
                        @Override
                        public MetricGroup getMetricGroup() {
                            return context.metricGroup();
                        }

                        @Override
                        public UserCodeClassLoader getUserCodeClassLoader() {
                            return context.getUserCodeClassLoader();
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize deserializer", e);
        }

        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "qfile-source-timer");
                            thread.setDaemon(true);
                            return thread;
                        });

        nextReadTime = System.currentTimeMillis() + pause;
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (currentSplit == null) {
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        if (splitFinished) {
            return InputStatus.END_OF_INPUT;
        }

        // ! throttling is handled by isAvailable(), never sleep in the mailbox thread
        if (System.currentTimeMillis() < nextReadTime) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        collector.setOutput(output);
        boolean emitted = "rand".equals(scanType) ? pollRand(collector) : pollSequential(collector);
        collector.setOutput(null);

        if (emitted) {
            nextReadTime = System.currentTimeMillis() + interval;
            // next record is only available after the interval
            return interval > 0 ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        }

        if ("once".equals(scanType)) {
            // end of file reached
            splitFinished = true;
            closeCurrentReader();
            return InputStatus.END_OF_INPUT;
        }

        // no new data yet, check the file again later
        nextReadTime = System.currentTimeMillis() + Math.max(interval, IDLE_WAIT_MS);
        return InputStatus.NOTHING_AVAILABLE;
    }

    /**
     * Read the next non blank line, blank lines are skipped instead of being treated as the end of
     * the file.
     *
     * @return true if at least one record was emitted
     */
    private boolean pollSequential(Collector<T> out) throws IOException {
        if (currentReader == null) {
            return false;
        }

        String line;
        while ((line = currentReader.readLine()) != null) {
            position += 1;
            if (StringUtils.isBlank(line)) {
                continue;
            }

            LOG.debug("QFile Source: {}", line);
            deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8), out);
            return true;
        }

        return false;
    }

    private boolean pollRand(Collector<T> out) throws IOException {
        if (fileLines == null || fileLines.isEmpty()) {
            return false;
        }

        String line = fileLines.get(random.nextInt(fileLines.size()));
        if (StringUtils.isBlank(line)) {
            return false;
        }

        LOG.debug("QFile Source: {}", line);
        deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8), out);
        return true;
    }

    @Override
    public List<QFileSourceSplit> snapshotState(long checkpointId) {
        if (splitFinished || currentSplit == null) {
            return Collections.emptyList();
        }

        // ! record the current position so that a restore does not read the file from scratch
        return Collections.singletonList(
                new QFileSourceSplit(
                        currentSplit.splitId(),
                        currentSplit.getFilePath(),
                        position,
                        currentSplit.getEndOffset()));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (currentSplit == null) {
            return noMoreSplits ? AVAILABLE : splitAssigned;
        }

        long delay = nextReadTime - System.currentTimeMillis();
        if (delay <= 0) {
            return AVAILABLE;
        }

        CompletableFuture<Void> available = new CompletableFuture<>();
        scheduler.schedule(() -> available.complete(null), delay, TimeUnit.MILLISECONDS);
        return available;
    }

    @Override
    public void addSplits(List<QFileSourceSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }

        if (splits.size() > 1) {
            LOG.warn(
                    "QFile source only handles one split per reader, ignore {}",
                    splits.subList(1, splits.size()));
        }

        // Take the first split (only one split per reader)
        QFileSourceSplit split = splits.get(0);
        this.currentSplit = split;
        this.splitFinished = false;
        this.position = 0;

        try {
            if ("rand".equals(scanType)) {
                // Load all lines into memory for random access
                Path filePath = Paths.get(split.getFilePath());
                this.fileLines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
                LOG.debug("Loaded {} lines from file: {}", fileLines.size(), split.getFilePath());
            } else {
                // Open buffered reader for sequential reading
                closeCurrentReader();
                this.currentReader =
                        Files.newBufferedReader(
                                Paths.get(split.getFilePath()), StandardCharsets.UTF_8);
                skipTo(split.getStartOffset());
                LOG.debug(
                        "Opened file for reading: {} from line {}",
                        split.getFilePath(),
                        split.getStartOffset());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file: " + split.getFilePath(), e);
        }

        splitAssigned.complete(null);
    }

    /** Restore the reader to the checkpointed line. */
    private void skipTo(long startOffset) throws IOException {
        while (position < startOffset && currentReader.readLine() != null) {
            position += 1;
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.debug("No more splits to process");
        noMoreSplits = true;
        splitAssigned.complete(null);
    }

    @Override
    public void close() throws Exception {
        closeCurrentReader();
        if (fileLines != null) {
            fileLines.clear();
            fileLines = null;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    private void closeCurrentReader() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException e) {
                LOG.warn("Error closing reader", e);
            }
            currentReader = null;
        }
    }

    /** Forwards the deserialized records to the reader output, dropping null records. */
    private static class OutputCollector<T> implements Collector<T> {

        private ReaderOutput<T> output;

        private void setOutput(ReaderOutput<T> output) {
            this.output = output;
        }

        @Override
        public void collect(T record) {
            // ! deserialization schemas may return null for records they want to drop
            if (record != null) {
                output.collect(record);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}
