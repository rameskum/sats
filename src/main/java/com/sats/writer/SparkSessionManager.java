package com.sats.writer;

import com.sats.config.SatsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Manages Spark session lifecycle with lazy initialisation and
 * fail-then-refresh recovery (Section 6.1).
 * <p>
 * Token rotation is handled by scheduling a refresh at 80% of the token TTL.
 * On unexpected 401, an immediate synchronous refresh is triggered.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SparkSessionManager {

    private final SatsProperties satsProperties;

    // TODO: LazyInitializer<SparkSession> with fail-then-refresh logic
    //       TokenProvider integration for PAT / OAuth lifecycle

    public void ensureSession() {
        log.debug("Ensuring Spark session is active (token refresh at {}% TTL)",
                satsProperties.spark().tokenRefreshPercent());
    }
}
