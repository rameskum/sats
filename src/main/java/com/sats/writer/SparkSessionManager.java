package com.sats.writer;

import com.sats.config.SatsProperties;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the Spark session lifecycle with lazy initialisation and
 * proactive token rotation.
 *
 * <h3>Lazy init</h3>
 * The session is not created until the first {@link #getSession()} call.
 * Double-checked locking (via {@code synchronized initSession()} +
 * {@link AtomicReference}) ensures exactly one session is created even
 * under concurrent write pressure.
 *
 * <h3>Token rotation</h3>
 * A background task fires at {@code tokenRefreshPercent}% of {@code tokenTtl}
 * (e.g. 80% × 1h = 48 min).  It calls {@link #invalidateAndRefresh()}, which
 * stops the old session and rebuilds it — picking up a fresh token on
 * {@code SparkSession.builder().getOrCreate()}.
 *
 * <h3>Fail-then-refresh</h3>
 * Write strategies call {@link #invalidateAndRefresh()} on any session-level
 * exception (e.g. expired PAT, lost cluster connection) before retrying,
 * instead of propagating stale-session errors indefinitely.
 */
@Component
@Slf4j
public class SparkSessionManager {

    private final SatsProperties satsProperties;
    private final ThreadPoolTaskScheduler taskScheduler;
    private final AtomicReference<SparkSession> sessionRef = new AtomicReference<>();

    public SparkSessionManager(SatsProperties satsProperties,
                               ThreadPoolTaskScheduler taskScheduler) {
        this.satsProperties = satsProperties;
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    void scheduleTokenRotation() {
        var spark = satsProperties.spark();
        long delayMs = (long) (spark.tokenTtl().toMillis() * spark.tokenRefreshPercent() / 100.0);
        log.info("Scheduling Spark token rotation every {}ms ({}% of {} TTL)",
                delayMs, spark.tokenRefreshPercent(), spark.tokenTtl());
        taskScheduler.scheduleWithFixedDelay(this::rotateToken, Duration.ofMillis(delayMs));
    }

    // ── Public API ────────────────────────────────────────────────────

    /**
     * Returns the active Spark session, initialising it lazily on first call.
     */
    public SparkSession getSession() {
        SparkSession session = sessionRef.get();
        if (session == null || session.sparkContext().isStopped()) {
            return initSession();
        }
        return session;
    }

    /**
     * Stops the current session and immediately creates a fresh one.
     * Called by write strategies on session-level failures and by the
     * scheduled token rotation task.
     */
    public void invalidateAndRefresh() {
        log.warn("Invalidating Spark session and reinitialising...");
        SparkSession old = sessionRef.getAndSet(null);
        if (old != null && !old.sparkContext().isStopped()) {
            try {
                old.stop();
            } catch (Exception e) {
                log.warn("Error while stopping stale Spark session: {}", e.getMessage());
            }
        }
        initSession();
    }

    // ── Internal ──────────────────────────────────────────────────────

    private void rotateToken() {
        log.info("Token TTL threshold reached — rotating Spark session");
        invalidateAndRefresh();
    }

    private synchronized SparkSession initSession() {
        // Double-checked: another thread may have already initialised by the time
        // this thread acquires the lock.
        SparkSession existing = sessionRef.get();
        if (existing != null && !existing.sparkContext().isStopped()) {
            return existing;
        }

        var spark = satsProperties.spark();
        log.info("Initialising Spark session: master={}, app={}", spark.master(), spark.appName());

        SparkSession session = SparkSession.builder()
                .master(spark.master())
                .appName(spark.appName())
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        sessionRef.set(session);
        log.info("Spark session ready (master={})", spark.master());
        return session;
    }
}
