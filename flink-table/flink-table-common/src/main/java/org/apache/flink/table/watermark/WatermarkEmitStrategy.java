package org.apache.flink.table.watermark;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Arrays;

/** The strategy for emitting watermark. */
@PublicEvolving
public enum WatermarkEmitStrategy {
    ON_EVENT("on-event"),
    ON_PERIODIC("on-periodic"),
    ;

    private final String alias;

    public String getAlias() {
        return alias;
    }

    WatermarkEmitStrategy(String alias) {
        this.alias = alias;
    }

    public boolean isOnEvent() {
        return this == ON_EVENT;
    }

    public boolean isOnPeriodic() {
        return this == ON_PERIODIC;
    }

    @Override
    public String toString() {
        return this.alias;
    }

    public static WatermarkEmitStrategy lookup(String alias) {
        return Arrays.stream(values())
                .filter(strategy -> strategy.getAlias().equalsIgnoreCase(alias))
                .findFirst()
                .orElseThrow(
                        () -> {
                            String msg =
                                    String.format(
                                            "Cannot find enum of watermark emit strategy with alias: %s",
                                            alias);
                            return new RuntimeException(msg);
                        });
    }
}
