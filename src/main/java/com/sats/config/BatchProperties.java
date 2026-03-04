package com.sats.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Per-dataset batching configuration.
 */
@ConfigurationProperties(prefix = "sats.batch")
public record BatchProperties(
        String byteTarget,
        int maxRecords,
        Duration maxTimer
) {

    /** Sensible defaults when YAML entries are absent. */
    public BatchProperties {
        if (byteTarget == null) byteTarget = "128MB";
        if (maxRecords <= 0) maxRecords = 50_000;
        if (maxTimer == null) maxTimer = Duration.ofSeconds(60);
    }
}
