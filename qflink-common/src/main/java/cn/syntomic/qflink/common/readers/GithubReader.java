package cn.syntomic.qflink.common.readers;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GithubReader implements ReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(GithubReader.class);

    private static final String URL_FORMAT = "https://raw.githubusercontent.com/%s/%s/%s/%s";

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("github.username")
                    .stringType()
                    .defaultValue("syntomic")
                    .withDescription("Github username");

    public static final ConfigOption<String> REPOSITORY =
            ConfigOptions.key("github.repository")
                    .stringType()
                    .defaultValue("qflink-sqls")
                    .withDescription("Github username");

    public static final ConfigOption<String> BRANCH =
            ConfigOptions.key("github.branch")
                    .stringType()
                    .defaultValue("main")
                    .withDescription("Github project branch");

    @Override
    public InputStream readInputStream(String filePath, Configuration conf) throws IOException {
        URL url =
                new URL(
                        String.format(
                                URL_FORMAT,
                                conf.get(USERNAME),
                                conf.get(REPOSITORY),
                                conf.get(BRANCH),
                                filePath));
        LOG.info("Read file {} from github", url);
        return url.openStream();
    }
}
