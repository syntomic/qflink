package cn.syntomic.qflink.common.configuration;

import static cn.syntomic.qflink.common.configuration.PropsOptions.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.syntomic.qflink.common.readers.ReaderFactory;

/** Job Configuration Factory */
public class ConfigurationFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationFactory.class);

    /** Command line configuration */
    private final Configuration commandConf;

    private ConfigurationFactory(Configuration commandConf) {
        this.commandConf = commandConf;
    }

    /**
     * Combine command line args and property files to job Configuration
     *
     * <p><b>Command line args > Specific property file > Default property file</b>
     *
     * @return
     * @throws IOException
     */
    public Configuration argsWithDefault() throws IOException {
        Configuration defaultConf = getDefaultConf(commandConf);

        // may need command and default conf to get specific conf
        Configuration defaultConfClone = defaultConf.clone();
        defaultConfClone.addAll(commandConf);
        Configuration specificConf = getSpecificConf(defaultConfClone);

        // merge
        defaultConf.addAll(specificConf);
        defaultConf.addAll(commandConf);
        return defaultConf;
    }

    /**
     * Accord command configuration to get default configuration
     *
     * @param commandConf
     * @return
     */
    @SuppressWarnings(value = {"unchecked", "rawtypes"})
    public Configuration getDefaultConf(Configuration commandConf) {
        try {
            String filePath =
                    Paths.get(
                                    String.format(
                                            commandConf.get(PROPS_FORMAT_DIR),
                                            commandConf.get(ENV).toString().toLowerCase()),
                                    commandConf.get(PROPS_DEFAULT))
                            .toString();

            Properties props = new Properties();
            // ! default props must in resource dir
            props.load(ReaderFactory.of("resource").readInputStream(filePath, commandConf));
            return Configuration.fromMap((Map) props);
        } catch (Exception e) {
            LOG.warn("Cannot find default properties file", e);
            return new Configuration();
        }
    }

    /**
     * Accord command and default merge configuration to get specific configuration
     *
     * @param mergeConf
     * @return
     * @throws IOException
     */
    @SuppressWarnings(value = {"unchecked", "rawtypes"})
    public Configuration getSpecificConf(Configuration mergeConf) throws IOException {
        String propsFile = mergeConf.get(PROPS_FILE);
        String propsDir =
                String.format(
                        mergeConf.get(PROPS_FORMAT_DIR),
                        mergeConf.get(ENV).toString().toLowerCase());
        String readType = mergeConf.get(PROPS_READ_TYPE);

        String filePath = null;
        if (propsFile == null) {
            return new Configuration();
        } else {
            // relative path
            filePath = Paths.get(propsDir, propsFile).toString();
        }

        Properties props = new Properties();
        props.load(ReaderFactory.of(readType).readInputStream(filePath, mergeConf));
        return Configuration.fromMap((Map) props);
    }

    /**
     * Create job configuration factory by default args parser
     *
     * @param args command line args
     * @return
     */
    public static ConfigurationFactory of(String[] args) throws IOException {
        Configuration commandConf = new DefaultArgsParserSelector().parseArgs(args);
        return new ConfigurationFactory(commandConf);
    }

    /**
     * Create job configuration factory by specific args parser
     *
     * @param args command line args
     * @param argsParser
     * @return
     */
    public static ConfigurationFactory of(String[] args, ArgsParser argsParser) throws IOException {
        Configuration commandConf = argsParser.parseArgs(args);
        return new ConfigurationFactory(commandConf);
    }
}
