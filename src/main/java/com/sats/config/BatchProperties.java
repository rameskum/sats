package com.sats.config;

import com.sats.domain.enums.FlushProfile;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Per-dataset batching configuration.
 * Defaults align with the "near-real-time" profile from the design document.
 */
@ConfigurationProperties(prefix = "sats.batch")
public record BatchProperties(
        FlushProfile defaultProfile,
        String byteTarget,
        int maxRecords,
        Duration maxTimer,
        double highWaterMarkMultiplier
) {

    /** Sensible defaults when YAML entries are absent. */
    public BatchProperties {
        if (defaultProfile == null) defaultProfile = FlushProfile.NEAR_REAL_TIME;
        if (byteTarget == null) byteTarget = "128MB";
        if (maxRecords <= 0) maxRecords = 50_000;
        if (maxTimer == null) maxTimer = Duration.ofSeconds(60);
        if (highWaterMarkMultiplier <= 0) highWaterMarkMultiplier = 2.0;
    }
}
