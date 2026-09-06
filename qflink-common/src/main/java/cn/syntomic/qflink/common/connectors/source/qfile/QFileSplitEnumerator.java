package cn.syntomic.qflink.common.connectors.source.qfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    /** ! a reader must be signalled once only, a finished task cannot receive an event anymore. */
    private final Set<Integer> signalledReaders;

    public QFileSplitEnumerator(
            SplitEnumeratorContext<QFileSourceSplit> context,
            String filePath,
            @Nullable QFileSplitEnumeratorState state) {
        this.context = context;
        this.filePath = filePath;
        this.remainingSplits = new ArrayList<>();
        this.signalledReaders = new HashSet<>();

        if (state != null && !state.getRemainingSplits().isEmpty()) {
            this.remainingSplits.addAll(state.getRemainingSplits());
        } else if (state == null) {
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
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<QFileSourceSplit> splits, int subtaskId) {
        LOG.debug("Adding back {} splits for subtask {}", splits.size(), subtaskId);
        remainingSplits.addAll(splits);
        // the subtask is restarted, it has to be signalled again
        signalledReaders.remove(subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader for subtask: {}", subtaskId);
        assignSplits();
    }

    private void assignSplits() {
        // Assign splits to available readers
        for (int reader : context.registeredReaders().keySet()) {
            if (remainingSplits.isEmpty()) {
                break;
            }

            if (!signalledReaders.contains(reader)) {
                QFileSourceSplit split = remainingSplits.remove(0);
                context.assignSplit(split, reader);
                LOG.debug("Assigned split {} to reader {}", split.splitId(), reader);
            }
        }

        if (!remainingSplits.isEmpty()) {
            // wait for the readers which will take the remaining splits
            return;
        }

        // ! readers without a split must be signalled too, otherwise they never finish
        for (int reader : context.registeredReaders().keySet()) {
            if (signalledReaders.add(reader)) {
                context.signalNoMoreSplits(reader);
            }
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
