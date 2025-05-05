package cn.syntomic.qflink.common.configuration;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ParameterTool;

public class UnixStyleArgsParser implements ArgsParser {

    @Override
    public Configuration parseArgs(String[] args) throws IOException {
        ParameterTool paramTool = ParameterTool.fromArgs(args);
        return Configuration.fromMap(paramTool.toMap());
    }
}
