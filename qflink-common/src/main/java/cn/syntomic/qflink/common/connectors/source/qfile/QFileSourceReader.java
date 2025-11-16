package cn.syntomic.qflink.common.connectors.source.qfile;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SourceReader for QFile that reads from file based on scan type. */
public class QFileSourceReader<T> implements SourceReader<T, QFileSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(QFileSourceReader.class);

    private final SourceReaderContext context;
    private final DeserializationSchema<T> deserializer;
    private final String scanType;
    private final long interval;
    private final long pause;

    private BufferedReader currentReader;
    private QFileSourceSplit currentSplit;
    private boolean splitFinished = true;
    private long lastReadTime = 0;
    private boolean paused = false;

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

        // Handle pause
        if (pause > 0 && !paused) {
            paused = true;
            lastReadTime = System.currentTimeMillis() + pause;
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        // Handle pause
        if (paused && System.currentTimeMillis() < lastReadTime) {
            Thread.sleep(100);
            return InputStatus.NOTHING_AVAILABLE;
        }
        paused = false;

        // Check if we need to wait based on interval
        long currentTime = System.currentTimeMillis();
        if (lastReadTime > 0 && currentTime - lastReadTime < interval) {
            Thread.sleep(Math.min(100, interval - (currentTime - lastReadTime)));
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (splitFinished) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        try {
            boolean hasData = false;
            switch (scanType) {
                case "once":
                    hasData = pollOnce(output);
                    break;
                case "rand":
                    hasData = pollRand(output);
                    break;
                case "cont":
                default:
                    hasData = pollCont(output);
                    break;
            }

            if (hasData) {
                lastReadTime = System.currentTimeMillis();
                return InputStatus.MORE_AVAILABLE;
            } else {
                if ("once".equals(scanType)) {
                    splitFinished = true;
                    return InputStatus.END_OF_INPUT;
                } else {
                    Thread.sleep(100);
                    return InputStatus.NOTHING_AVAILABLE;
                }
            }
        } catch (Exception e) {
            LOG.error("Error reading from file", e);
            throw e;
        }
    }

    private boolean pollOnce(ReaderOutput<T> output) throws IOException {
        if (currentReader == null) {
            return false;
        }

        String line = currentReader.readLine();
        if (line != null) {
            if (StringUtils.isNotBlank(line)) {
                LOG.debug("QFile Source: {}", line);
                T record = deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8));
                output.collect(record);
                return true;
            }
            return false;
        } else {
            // Reached end of file
            closeCurrentReader();
            return false;
        }
    }

    private boolean pollRand(ReaderOutput<T> output) throws IOException {
        if (fileLines == null || fileLines.isEmpty()) {
            return false;
        }

        int randomLine = random.nextInt(fileLines.size());
        String line = fileLines.get(randomLine);

        if (StringUtils.isNotBlank(line)) {
            LOG.debug("QFile Source: {}", line);
            T record = deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8));
            output.collect(record);
            return true;
        }
        return false;
    }

    private boolean pollCont(ReaderOutput<T> output) throws IOException {
        if (currentReader == null) {
            return false;
        }

        String line = currentReader.readLine();
        if (line != null) {
            if (StringUtils.isNotBlank(line)) {
                LOG.debug("QFile Source: {}", line);
                T record = deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8));
                output.collect(record);
                return true;
            }
            return false;
        } else {
            // No new data available, but keep the reader open
            return false;
        }
    }

    @Override
    public List<QFileSourceSplit> snapshotState(long checkpointId) {
        List<QFileSourceSplit> splits = new ArrayList<>();
        if (!splitFinished && currentSplit != null) {
            splits.add(currentSplit);
        }
        return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<QFileSourceSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }

        // Take the first split (typically only one split per reader)
        QFileSourceSplit split = splits.get(0);
        this.currentSplit = split;
        this.splitFinished = false;

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
                        new BufferedReader(
                                new InputStreamReader(
                                        new FileInputStream(split.getFilePath()),
                                        StandardCharsets.UTF_8));
                LOG.debug("Opened file for reading: {}", split.getFilePath());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file: " + split.getFilePath(), e);
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.debug("No more splits to process");
    }

    @Override
    public void close() throws Exception {
        closeCurrentReader();
        if (fileLines != null) {
            fileLines.clear();
            fileLines = null;
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
}
