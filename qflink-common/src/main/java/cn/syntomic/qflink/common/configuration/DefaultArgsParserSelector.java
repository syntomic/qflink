package cn.syntomic.qflink.common.configuration;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;

public class DefaultArgsParserSelector implements ArgsParser {

    @Override
    public Configuration parseArgs(String[] args) throws IOException {
        switch (args.length) {
            case 1:
            case 0:
                return new JsonStyleArgsParser().parseArgs(args);
            default:
                return new UnixStyleArgsParser().parseArgs(args);
        }
    }
}
