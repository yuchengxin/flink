package org.apache.flink.table.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;

/** Pojo class for watermark configs from table options or 'OPTIONS' hint. */
@Internal
public class WatermarkParams implements Serializable {
    private static final long serialVersionUID = 1L;

    private WatermarkEmitStrategy emitStrategy;
    private int emitOnEventGap;

    public WatermarkParams() {}

    public WatermarkParams(WatermarkEmitStrategy emitStrategy, int emitOnEventGap) {
        this.emitStrategy = emitStrategy;
        this.emitOnEventGap = emitOnEventGap;
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
                + '}';
    }

    /** Builder of WatermarkHintParams. */
    public static class WatermarkParamsBuilder {
        private WatermarkEmitStrategy emitStrategy =
                FactoryUtil.WATERMARK_EMIT_STRATEGY.defaultValue();
        private int emitOnEventGap = FactoryUtil.WATERMARK_EMIT_ON_EVENT_GAP.defaultValue();

        public WatermarkParamsBuilder emitStrategy(WatermarkEmitStrategy emitStrategy) {
            this.emitStrategy = emitStrategy;
            return this;
        }

        public WatermarkParamsBuilder emitOnEventGap(int emitOnEventGap) {
            this.emitOnEventGap = emitOnEventGap;
            return this;
        }

        public WatermarkParams build() {
            return new WatermarkParams(emitStrategy, emitOnEventGap);
        }
    }
}
