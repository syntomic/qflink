package cn.syntomic.qflink.common.configuration;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser.Feature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import cn.syntomic.qflink.common.readers.ReaderFactory;

public class JsonStyleArgsParser implements ArgsParser {

    /** Example configuration file */
    private static final String EXAMPLE_JSON = "example.json";

    @Override
    public Configuration parseArgs(String[] args) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> argsMap;

        // ! not support nested json
        if (args.length == 1) {
            argsMap = mapper.readValue(args[0], new TypeReference<Map<String, String>>() {});
        } else {
            mapper.configure(Feature.ALLOW_COMMENTS, true);
            argsMap =
                    mapper.readValue(
                            ReaderFactory.defaultReader().readFile(EXAMPLE_JSON),
                            new TypeReference<Map<String, String>>() {});
        }

        return Configuration.fromMap(argsMap);
    }
}
