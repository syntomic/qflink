package cn.syntomic.qflink.common.readers;

import java.util.function.Function;

public class DefaultReaderSelector implements Function<String, ReaderFactory> {

    @Override
    public ReaderFactory apply(String readType) {
        switch (readType.toLowerCase()) {
            case "github":
                return new GithubReader();
            case "file":
                return new FileReader();
            case "resource":
            default:
                return new ResourceReader();
        }
    }
}
