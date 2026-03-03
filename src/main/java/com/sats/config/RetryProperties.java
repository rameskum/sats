package com.sats.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Exponential-backoff retry configuration for Spark write failures.
 */
@ConfigurationProperties(prefix = "sats.retry")
public record RetryProperties(
        int maxAttempts,
        Duration initialBackoff,
        double backoffMultiplier
) {

    public RetryProperties {
        if (maxAttempts <= 0) maxAttempts = 5;
        if (initialBackoff == null) initialBackoff = Duration.ofSeconds(1);
        if (backoffMultiplier <= 0) backoffMultiplier = 2.0;
    }
}
