package cn.syntomic.qflink.web.channel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Appends a record to the file a qfile source tails.
 *
 * <p>The source is only notified by new lines, so the file is the whole history of the channel: a
 * job which restarts replays it from the beginning and ends up with the same rules.
 */
public class FilePublisher implements Publisher {

    private final Path path;

    public FilePublisher(Path path) {
        this.path = path;
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            if (!Files.exists(path)) {
                Files.createFile(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create " + path, e);
        }
    }

    @Override
    public synchronized void publish(String record) {
        try {
            Files.writeString(
                    path,
                    record + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot append to " + path, e);
        }
    }

    @Override
    public String describe() {
        return "qfile " + path;
    }

    @Override
    public void close() {
        // nothing to keep open, every append opens the file
    }
}
