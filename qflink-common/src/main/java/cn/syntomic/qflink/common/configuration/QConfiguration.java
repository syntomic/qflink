package cn.syntomic.qflink.common.configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.description.Description;

public class QConfiguration extends Configuration {

    public QConfiguration() {
        super();
    }

    public QConfiguration(Configuration other) {
        super(other);
    }

    public static QConfiguration fromMap(Map<String, String> map) {
        final QConfiguration configuration = new QConfiguration();
        map.forEach(configuration::setString);
        return configuration;
    }

    /**
     * Get configuration by prefix
     *
     * <p><b>Do not use it in job execution!<b>
     *
     * <p>
     *
     * @param <T>
     * @param prefix
     * @param option
     * @return
     */
    public <T> T get(String prefix, ConfigOption<T> option) {
        return get(prefixOption(option, prefix));
    }

    /**
     * Get configuration by prefix and override default
     *
     * <p><b>Do not use it in job execution!<b>
     *
     * <p>
     *
     * @param <T>
     * @param prefix
     * @param option
     * @param overrideDefault
     * @return
     */
    public <T> T get(String prefix, ConfigOption<T> option, T overrideDefault) {
        return getOptional(prefixOption(option, prefix)).orElse(overrideDefault);
    }

    /**
     * Use reflection to create option with prefix and original option key fallback to deprecated
     * key
     *
     * <p><b>Do not use it in job execution!<b>
     *
     * <p>
     *
     * @param <T>
     * @param option
     * @param prefix
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T> ConfigOption<T> prefixOption(ConfigOption<T> option, String prefix) {
        try {
            Constructor<?> configOptionCons =
                    ConfigOption.class.getDeclaredConstructor(
                            String.class,
                            Class.class,
                            Description.class,
                            Object.class,
                            boolean.class,
                            FallbackKey[].class);

            Field clazzField = ConfigOption.class.getDeclaredField("clazz");
            Field isListField = ConfigOption.class.getDeclaredField("isList");
            Method createDeprecatedKey =
                    FallbackKey.class.getDeclaredMethod("createDeprecatedKey", String.class);

            clazzField.setAccessible(true);
            isListField.setAccessible(true);
            configOptionCons.setAccessible(true);
            createDeprecatedKey.setAccessible(true);

            List<FallbackKey> deprecatedKeys = new ArrayList<>();
            deprecatedKeys.add((FallbackKey) createDeprecatedKey.invoke(null, option.key()));
            if (option.hasFallbackKeys()) {
                for (FallbackKey dk : option.fallbackKeys()) {
                    // ! prefix fallback key > fallback key
                    deprecatedKeys.add(
                            (FallbackKey)
                                    createDeprecatedKey.invoke(null, prefix + "." + dk.getKey()));
                    deprecatedKeys.add((FallbackKey) createDeprecatedKey.invoke(null, dk.getKey()));
                }
            }

            FallbackKey[] deprecated = deprecatedKeys.toArray(new FallbackKey[0]);
            return (ConfigOption<T>)
                    configOptionCons.newInstance(
                            prefix + "." + option.key(),
                            clazzField.get(option),
                            option.description(),
                            option.defaultValue(),
                            isListField.get(option),
                            deprecated);
        } catch (Exception e) {
            throw new SecurityException("Unable to get access to private methods");
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QConfiguration) {
            Configuration other = (Configuration) obj;
            return other.equals(this);
        } else {
            return false;
        }
    }
}
