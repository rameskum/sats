package com.sats.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Threading and concurrency configuration.
 * Governs the platform-thread write pool and virtual-thread carrier pool.
 */
@ConfigurationProperties(prefix = "sats.threading")
public record ThreadingProperties(
        int writePoolSize,
        int writeQueueCapacity,
        int carrierPoolSize,
        Duration pinAlertThreshold
) {

    public ThreadingProperties {
        if (writePoolSize <= 0) writePoolSize = 8;
        if (writeQueueCapacity <= 0) writeQueueCapacity = writePoolSize * 2;
        if (carrierPoolSize <= 0) carrierPoolSize = Runtime.getRuntime().availableProcessors();
        if (pinAlertThreshold == null) pinAlertThreshold = Duration.ofMillis(100);
    }
}
