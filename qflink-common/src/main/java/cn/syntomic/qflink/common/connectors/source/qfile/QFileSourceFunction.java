package cn.syntomic.qflink.common.connectors.source.qfile;

import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.INTERVAL;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.PATH;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.PAUSE;
import static cn.syntomic.qflink.common.connectors.source.qfile.QFileSourceOptions.SCAN_TYPE;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.common.configuration.QConfiguration;
import cn.syntomic.qflink.common.utils.FileUtil;

/** Custom read file and is helpful when developing */
public class QFileSourceFunction<T> extends RichSourceFunction<T>
        implements ResultTypeQueryable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(QFileSourceFunction.class);

    private static final String DEFAULT_DIR = "sources";

    private final String path;
    private final String scanType;
    private final long interval;
    private final long pause;

    private final DeserializationSchema<T> deserializer;

    private volatile boolean running = true;

    private QFileSourceFunction(
            String path,
            String readType,
            long interval,
            long pause,
            DeserializationSchema<T> deserializer) {

        this.path = path;
        this.scanType = readType;
        this.interval = interval;
        this.pause = pause;

        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        deserializer.open(
                RuntimeContextInitializationContextAdapters.deserializationAdapter(
                        getRuntimeContext()));
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        if (pause > 0) {
            Thread.sleep(pause);
        }

        switch (scanType) {
            case "once":
                onceRun(ctx);
                break;
            case "rand":
                randRun(ctx);
                break;
            case "cont":
            default:
                contRun(ctx);
                break;
        }

        running = false;
    }

    /**
     * Read file once in order
     *
     * @param ctx
     * @throws Exception
     */
    public void onceRun(SourceContext<T> ctx) throws Exception {

        String line;
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            while ((line = reader.readLine()) != null) {
                LOG.debug("QFile Source : {}", line);
                if (StringUtils.isNotBlank(line)) {
                    ctx.collect(deserializer.deserialize(line.getBytes()));
                }
                Thread.sleep(interval);
            }
        }
    }

    /**
     * Read file random
     *
     * @param ctx
     * @throws Exception
     */
    public void randRun(SourceContext<T> ctx) throws Exception {

        Path filePath = Paths.get(path);
        List<String> lines = Files.readAllLines(filePath);
        int numOfLine = lines.size();
        Random r = new Random();

        int randomLine;
        while (running) {
            randomLine = r.nextInt(numOfLine);
            LOG.debug("QFile Source : {}", randomLine);
            if (StringUtils.isNotBlank(lines.get(randomLine))) {
                ctx.collect(deserializer.deserialize(lines.get(randomLine).getBytes()));
            }
            Thread.sleep(interval);
        }
    }

    /**
     * Read file continuous and can monitor new line
     *
     * @param ctx
     * @throws Exception
     */
    public void contRun(SourceContext<T> ctx) throws Exception {

        String line;
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {

            while (running) {
                line = reader.readLine();
                if (StringUtils.isNotBlank(line)) {
                    LOG.debug("QFile Source : {}", line);
                    ctx.collect(deserializer.deserialize(line.getBytes()));
                    Thread.sleep(interval);
                } else {
                    Thread.sleep(1000);
                }
            }
        }
    }

    /**
     * Compatible with kafka scan mode config
     *
     * @param scanMode
     * @return
     */
    private static String scanModeWrapper(String scanMode) {
        return "latest-offset".equals(scanMode) ? "cont" : scanMode;
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    public static <T> QFileSourceFunction<T> of(
            QConfiguration conf, DeserializationSchema<T> deserializer, String sourceName) {

        return new QFileSourceFunction<>(
                FileUtil.getRelPath(DEFAULT_DIR, conf.get(sourceName, PATH)).toString(),
                scanModeWrapper(conf.get(sourceName, SCAN_TYPE).toLowerCase()),
                conf.get(sourceName, INTERVAL).toMillis(),
                conf.get(sourceName, PAUSE).toMillis(),
                deserializer);
    }

    public static <T> QFileSourceFunction<T> of(
            String path,
            String scanType,
            long interval,
            long pause,
            DeserializationSchema<T> deserializer) {

        return new QFileSourceFunction<>(
                FileUtil.getRelPath(DEFAULT_DIR, path).toString(),
                scanModeWrapper(scanType),
                interval,
                pause,
                deserializer);
    }
}
