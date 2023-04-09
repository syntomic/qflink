package cn.syntomic.qflink.common.readers;

import java.io.IOException;
import java.io.InputStream;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceReader implements ReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceReader.class);

    @Override
    public InputStream readInputStream(String filePath, Configuration conf) throws IOException {
        LOG.info("Read file {} from resource", filePath);
        InputStream inputStream =
                ResourceReader.class.getClassLoader().getResourceAsStream(filePath);
        if (inputStream != null) {
            return inputStream;
        } else {
            throw new IOException(String.format("The resource %s could not be found", filePath));
        }
    }
}
