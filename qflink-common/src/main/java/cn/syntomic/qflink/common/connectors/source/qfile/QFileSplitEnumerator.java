package cn.syntomic.qflink.common.connectors.source.qfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SplitEnumerator for QFile source that manages file splits. */
public class QFileSplitEnumerator
        implements SplitEnumerator<QFileSourceSplit, QFileSplitEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(QFileSplitEnumerator.class);

    private final SplitEnumeratorContext<QFileSourceSplit> context;
    private final String filePath;
    private final List<QFileSourceSplit> remainingSplits;

    public QFileSplitEnumerator(
            SplitEnumeratorContext<QFileSourceSplit> context,
            String filePath,
            @Nullable QFileSplitEnumeratorState state) {
        this.context = context;
        this.filePath = filePath;
        this.remainingSplits = new ArrayList<>();

        if (state != null && state.getRemaingSplits() != null) {
            this.remainingSplits.addAll(state.getRemaingSplits());
        } else {
            // Create initial split
            this.remainingSplits.add(new QFileSourceSplit("qfile-split-0", filePath));
        }
    }

    @Override
    public void start() {
        LOG.debug("Starting QFileSplitEnumerator for file: {}", filePath);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Not used in this implementation
    }

    @Override
    public void addSplitsBack(List<QFileSourceSplit> splits, int subtaskId) {
        LOG.debug("Adding back {} splits for subtask {}", splits.size(), subtaskId);
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader for subtask: {}", subtaskId);
        assignSplits();
    }

    private void assignSplits() {
        if (remainingSplits.isEmpty()) {
            return;
        }

        // Assign splits to available readers
        for (int reader : context.registeredReaders().keySet()) {
            if (!remainingSplits.isEmpty()) {
                QFileSourceSplit split = remainingSplits.remove(0);
                context.assignSplit(split, reader);
                LOG.debug("Assigned split {} to reader {}", split.splitId(), reader);
            }
        }

        // Signal no more splits
        for (int reader : context.registeredReaders().keySet()) {
            context.signalNoMoreSplits(reader);
        }
    }

    @Override
    public QFileSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Snapshotting state at checkpoint: {}", checkpointId);
        return new QFileSplitEnumeratorState(new ArrayList<>(remainingSplits));
    }

    @Override
    public void close() throws IOException {
        LOG.debug("Closing QFileSplitEnumerator");
    }
}
