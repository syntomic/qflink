package cn.syntomic.qflink.web.channel;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tails the file a qfile sink writes, from the beginning when {@code web.replay} is set, so a page
 * which opens late still sees what already happened.
 *
 * <p>The qfile sink empties the file when the job starts, a file which shrank is therefore read
 * from the beginning again instead of being reported as corrupted.
 */
public class FileTailSubscriber implements Subscriber {

    private static final Logger LOG = LoggerFactory.getLogger(FileTailSubscriber.class);

    private static final long POLL_INTERVAL_MS = 200L;

    private final Path path;
    private final String name;
    private final boolean replay;

    private volatile boolean running = true;
    private Thread thread;

    public FileTailSubscriber(String name, Path path, boolean replay) {
        this.name = name;
        this.path = path;
        this.replay = replay;
    }

    @Override
    public void start(Consumer<String> onRecord) {
        thread = new Thread(() -> tail(onRecord), "qflink-web-tail-" + name);
        thread.setDaemon(true);
        thread.start();
    }

    private void tail(Consumer<String> onRecord) {
        // ! the first offset is only known once the file exists, it may be written after the start
        long position = -1L;

        while (running) {
            try {
                if (!Files.isReadable(path)) {
                    sleep();
                    continue;
                }

                long length = Files.size(path);
                if (position < 0) {
                    position = replay ? 0L : length;
                }
                if (length < position) {
                    // ! the sink emptied the file, a new job run started
                    LOG.info("The {} file was truncated, read it from the beginning", name);
                    position = 0L;
                }
                if (length == position) {
                    sleep();
                    continue;
                }

                position = read(position, onRecord);
            } catch (IOException e) {
                LOG.warn("Cannot tail {}", path, e);
                sleep();
            }
        }
    }

    /**
     * Read the lines which are complete, a half written line stays in the file for the next round
     *
     * @param position the offset the last round stopped at
     * @param onRecord
     * @return the new offset
     * @throws IOException
     */
    private long read(long position, Consumer<String> onRecord) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
            file.seek(position);

            String line;
            long consumed = position;
            while ((line = file.readLine()) != null) {
                long next = file.getFilePointer();
                if (next == file.length() && !endsWithNewLine(file)) {
                    // ! the sink is in the middle of a line, wait for the rest of it
                    break;
                }

                consumed = next;
                String record =
                        new String(
                                line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                if (!record.isBlank()) {
                    onRecord.accept(record);
                }
            }
            return consumed;
        }
    }

    private boolean endsWithNewLine(RandomAccessFile file) throws IOException {
        long length = file.length();
        if (length == 0) {
            return false;
        }

        long pointer = file.getFilePointer();
        file.seek(length - 1);
        int last = file.read();
        file.seek(pointer);
        return last == '\n' || last == '\r';
    }

    private void sleep() {
        try {
            Thread.sleep(POLL_INTERVAL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    @Override
    public String describe() {
        return "qfile " + path;
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }
}
