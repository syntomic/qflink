package cn.syntomic.qflink.common.readers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.flink.configuration.Configuration;

import org.apache.commons.io.IOUtils;

public interface ReaderFactory {

    /**
     * read file as string
     *
     * @param filePath
     * @param conf
     * @return
     * @throws IOException
     */
    default String readFile(String filePath, Configuration conf) throws IOException {
        return IOUtils.toString(readInputStream(filePath, conf), StandardCharsets.UTF_8);
    };

    /**
     * read file as string without configuration
     *
     * @param filePath
     * @return
     * @throws IOException
     */
    default String readFile(String filePath) throws IOException {
        return readFile(filePath, null);
    }

    /**
     * read file as input stream
     *
     * @param filePath
     * @param conf
     * @return
     * @throws IOException
     */
    InputStream readInputStream(String filePath, Configuration conf) throws IOException;

    /**
     * get default reader factory
     *
     * @return
     */
    public static ReaderFactory defaultReader() {
        return new ResourceReader();
    }

    /**
     * create reader factory by default selector
     *
     * @param filePath
     * @param readType
     * @return {@link ReaderFactory}
     */
    public static ReaderFactory of(String readType) {
        return new DefaultReaderSelector().apply(readType);
    }

    /**
     * create reader factory by specific selector
     *
     * @param filePath
     * @param readType
     * @param readerSelector
     * @return {@link ReaderFactory}
     */
    public static ReaderFactory of(String readType, ReaderSelector readerSelector) {
        return readerSelector.apply(readType);
    }
}
