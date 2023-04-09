package cn.syntomic.qflink.common.configuration;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;

public interface ArgsParser {

    /**
     * Parse command line args to configuration
     *
     * @param args
     * @return
     * @throws IOException
     */
    Configuration parseArgs(String[] args) throws IOException;
}
