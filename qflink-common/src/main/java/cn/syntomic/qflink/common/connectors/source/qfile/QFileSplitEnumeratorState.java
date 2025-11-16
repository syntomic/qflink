package cn.syntomic.qflink.common.connectors.source.qfile;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** State for {@link QFileSplitEnumerator} to support checkpoint. */
public class QFileSplitEnumeratorState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<QFileSourceSplit> remainingSplits;

    public QFileSplitEnumeratorState(List<QFileSourceSplit> remainingSplits) {
        this.remainingSplits = remainingSplits;
    }

    public List<QFileSourceSplit> getRemaingSplits() {
        return remainingSplits != null ? remainingSplits : Collections.emptyList();
    }
}
