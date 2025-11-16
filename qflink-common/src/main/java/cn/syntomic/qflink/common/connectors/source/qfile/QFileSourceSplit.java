package cn.syntomic.qflink.common.connectors.source.qfile;

import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceSplit;

/** A split representing a file or a portion of file reading task. */
public class QFileSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final String filePath;
    private final long startOffset;
    private final long endOffset;

    public QFileSourceSplit(String splitId, String filePath, long startOffset, long endOffset) {
        this.splitId = splitId;
        this.filePath = filePath;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public QFileSourceSplit(String splitId, String filePath) {
        this(splitId, filePath, 0, -1);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "QFileSourceSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", filePath='"
                + filePath
                + '\''
                + ", startOffset="
                + startOffset
                + ", endOffset="
                + endOffset
                + '}';
    }
}
