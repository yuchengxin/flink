package org.apache.flink.table.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.time.Duration;

/** Pojo class for watermark configs from table options or 'OPTIONS' hint. */
@Internal
public class WatermarkParams implements Serializable {
    private static final long serialVersionUID = 1L;

    private WatermarkEmitStrategy emitStrategy;
    private int emitOnEventGap;
    private String alignGroupName;
    private Duration alignMaxDrift;
    private Duration alignUpdateInterval;

    public WatermarkParams() {}

    public WatermarkParams(
            WatermarkEmitStrategy emitStrategy,
            int emitOnEventGap,
            String alignGroupName,
            Duration alignMaxDrift,
            Duration alignUpdateInterval) {
        this.emitStrategy = emitStrategy;
        this.emitOnEventGap = emitOnEventGap;
        this.alignGroupName = alignGroupName;
        this.alignMaxDrift = alignMaxDrift;
        this.alignUpdateInterval = alignUpdateInterval;
    }

    public WatermarkEmitStrategy getEmitStrategy() {
        return emitStrategy;
    }

    public void setEmitStrategy(WatermarkEmitStrategy emitStrategy) {
        this.emitStrategy = emitStrategy;
    }

    public int getEmitOnEventGap() {
        return emitOnEventGap;
    }

    public void setEmitOnEventGap(int emitOnEventGap) {
        this.emitOnEventGap = emitOnEventGap;
    }

    public String getAlignGroupName() {
        return alignGroupName;
    }

    public void setAlignGroupName(String alignGroupName) {
        this.alignGroupName = alignGroupName;
    }

    public Duration getAlignMaxDrift() {
        return alignMaxDrift;
    }

    public void setAlignMaxDrift(Duration alignMaxDrift) {
        this.alignMaxDrift = alignMaxDrift;
    }

    public Duration getAlignUpdateInterval() {
        return alignUpdateInterval;
    }

    public void setAlignUpdateInterval(Duration alignUpdateInterval) {
        this.alignUpdateInterval = alignUpdateInterval;
    }

    public boolean alignWatermarkEnabled() {
        return !StringUtils.isNullOrWhitespaceOnly(alignGroupName)
                && alignMaxDrift != null
                && isDurationPositive(alignMaxDrift)
                && alignUpdateInterval != null
                && isDurationPositive(alignUpdateInterval);
    }

    private boolean isDurationPositive(Duration duration) {
        return !duration.isNegative() && !duration.isZero();
    }

    public static WatermarkParamsBuilder builder() {
        return new WatermarkParamsBuilder();
    }

    @Override
    public String toString() {
        return "WatermarkParams{"
                + ", emitStrategy="
                + emitStrategy
                + ", emitOnEventGap="
                + emitOnEventGap
                + "alignGroupName='"
                + alignGroupName
                + '\''
                + ", alignMaxDrift="
                + alignMaxDrift
                + ", alignUpdateInterval="
                + alignUpdateInterval
                + '}';
    }

    /** Builder of WatermarkHintParams. */
    public static class WatermarkParamsBuilder {
        private WatermarkEmitStrategy emitStrategy =
                FactoryUtil.WATERMARK_EMIT_STRATEGY.defaultValue();
        private int emitOnEventGap = FactoryUtil.WATERMARK_EMIT_ON_EVENT_GAP.defaultValue();
        private String alignGroupName;
        private Duration alignMaxDrift = Duration.ZERO;
        private Duration alignUpdateInterval =
                FactoryUtil.WATERMARK_ALIGNMENT_UPDATE_INTERVAL.defaultValue();

        public WatermarkParamsBuilder emitStrategy(WatermarkEmitStrategy emitStrategy) {
            this.emitStrategy = emitStrategy;
            return this;
        }

        public WatermarkParamsBuilder emitOnEventGap(int emitOnEventGap) {
            this.emitOnEventGap = emitOnEventGap;
            return this;
        }

        public WatermarkParamsBuilder alignGroupName(String alignGroupName) {
            this.alignGroupName = alignGroupName;
            return this;
        }

        public WatermarkParamsBuilder alignMaxDrift(Duration alignMaxDrift) {
            this.alignMaxDrift = alignMaxDrift;
            return this;
        }

        public WatermarkParamsBuilder alignUpdateInterval(Duration alignUpdateInterval) {
            this.alignUpdateInterval = alignUpdateInterval;
            return this;
        }

        public WatermarkParams build() {
            return new WatermarkParams(
                    emitStrategy,
                    emitOnEventGap,
                    alignGroupName,
                    alignMaxDrift,
                    alignUpdateInterval);
        }
    }
}
