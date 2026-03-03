package com.sats.domain.enums;

import java.time.Duration;

/**
 * Pre-defined flush profiles from Section 8.5 of the design document.
 * Each profile balances file size against ingestion latency.
 */
public enum FlushProfile {

    REAL_TIME("32MB", 10_000, Duration.ofSeconds(10)),
    NEAR_REAL_TIME("128MB", 50_000, Duration.ofSeconds(60)),
    BATCH_OPTIMIZED("256MB", 100_000, Duration.ofSeconds(300));

    private final String byteTarget;
    private final int maxRecords;
    private final Duration maxTimer;

    FlushProfile(String byteTarget, int maxRecords, Duration maxTimer) {
        this.byteTarget = byteTarget;
        this.maxRecords = maxRecords;
        this.maxTimer = maxTimer;
    }

    public String byteTarget() { return byteTarget; }
    public int maxRecords() { return maxRecords; }
    public Duration maxTimer() { return maxTimer; }
}
