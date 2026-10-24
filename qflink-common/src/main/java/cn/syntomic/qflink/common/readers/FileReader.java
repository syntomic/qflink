package cn.syntomic.qflink.common.readers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read a file from the local file system, relative to {@code file.base-dir}.
 *
 * <p>A resource is copied into the classpath at build time, so an edited sql only takes effect
 * after a rebuild. Reading the source file directly lets a local run pick up the edit at once.
 */
public class FileReader implements ReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FileReader.class);

    public static final ConfigOption<String> BASE_DIR =
            ConfigOptions.key("file.base-dir")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Directory a relative file path is resolved against, the working"
                                    + " directory when empty.");

    @Override
    public InputStream readInputStream(String filePath, Configuration conf) throws IOException {
        // ! the working directory is kept for the job itself, e.g. the relative python files
        String baseDir = conf == null ? "" : conf.get(BASE_DIR);
        Path path = Paths.get(baseDir).resolve(filePath).toAbsolutePath();
        LOG.info("Read file {} from file system", path);
        if (!Files.isRegularFile(path)) {
            throw new IOException(String.format("The file %s could not be found", path));
        }
        return Files.newInputStream(path);
    }
}
