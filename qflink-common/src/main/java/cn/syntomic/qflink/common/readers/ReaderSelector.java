package cn.syntomic.qflink.common.readers;

import java.util.function.Function;

public interface ReaderSelector extends Function<String, ReaderFactory> {}
