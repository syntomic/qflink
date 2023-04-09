package cn.syntomic.qflink.common.utils;

import java.time.Duration;

import javax.annotation.Nullable;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import cn.syntomic.qflink.common.configuration.QConfiguration;

public class WatermarkUtil {

    public static final ConfigOption<Duration> OUT_OF_ORDERNESS =
            ConfigOptions.key("watermark.out-of-orderness")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("Upper bound on how far the events are out of order.");

    public static final ConfigOption<Duration> IDLE_TIMEOUT =
            ConfigOptions.key("watermark.idle-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Add an idle timeout to the watermark strategy.");

    public static final ConfigOption<String> ALIGN_GROUP =
            ConfigOptions.key("watermark.align_group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A group of sources to align watermarks.");

    public static final ConfigOption<Duration> MAX_DRIFT =
            ConfigOptions.key("watermark.max_drift")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Maximal drift, before we pause consuming from the source.");

    /**
     * Set max watermark in order to not effect other input
     *
     * @param <T>
     * @return
     */
    public static <T> WatermarkStrategy<T> setMax() {
        return WatermarkStrategy.<T>forGenerator(
                ctx ->
                        new WatermarkGenerator<T>() {
                            @Override
                            public void onEvent(
                                    T event, long eventTimestamp, WatermarkOutput output) {
                                // only need periodic emit
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                // ! watermark not in checkpoint, restart will lose watermark
                                output.emitWatermark(new Watermark(Long.MAX_VALUE));
                            }
                        });
    }

    /**
     * Set max out of orderness watermark strategy
     *
     * @param <T>
     * @param timestampAssigner
     * @param maxOutOfOrderness
     * @param idleTimeout
     * @param watermarkGroup
     * @param maxAllowedWatermarkDrift
     * @return
     */
    public static <T> WatermarkStrategy<T> setBoundedOutOfOrderness(
            SerializableTimestampAssigner<T> timestampAssigner,
            Duration maxOutOfOrderness,
            @Nullable Duration idleTimeout,
            @Nullable String watermarkGroup,
            @Nullable Duration maxAllowedWatermarkDrift) {

        WatermarkStrategy<T> watermarkStrategy =
                WatermarkStrategy.<T>forBoundedOutOfOrderness(maxOutOfOrderness)
                        .withTimestampAssigner(timestampAssigner);

        if (idleTimeout != null) {
            watermarkStrategy = watermarkStrategy.withIdleness(idleTimeout);
        }

        if (maxAllowedWatermarkDrift != null) {
            Preconditions.checkNotNull(
                    watermarkGroup, "The group of sources to align watermarks must not null");
            watermarkStrategy.withWatermarkAlignment(watermarkGroup, maxAllowedWatermarkDrift);
        }

        return watermarkStrategy;
    }

    /**
     * Set max out of orderness watermark strategy according job configuration
     *
     * @param <T>
     * @param timestampAssigner
     * @param conf
     * @param sourceName
     * @return
     */
    public static <T> WatermarkStrategy<T> setBoundedOutOfOrderness(
            SerializableTimestampAssigner<T> timestampAssigner,
            Configuration conf,
            String sourceName) {
        QConfiguration confWrapper = new QConfiguration(conf);

        return setBoundedOutOfOrderness(
                timestampAssigner,
                confWrapper.get(sourceName, OUT_OF_ORDERNESS),
                confWrapper.get(sourceName, IDLE_TIMEOUT),
                confWrapper.get(sourceName, ALIGN_GROUP),
                confWrapper.get(sourceName, MAX_DRIFT));
    }

    private WatermarkUtil() {}
}
