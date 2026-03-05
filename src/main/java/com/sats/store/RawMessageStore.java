package com.sats.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sats.config.SatsProperties;
import com.sats.domain.enums.MessageType;
import com.sats.domain.model.RawMessage;
import com.sats.writer.SparkSessionManager;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.Encoders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Captures every incoming Kafka record verbatim — before transformation —
 * and periodically flushes them to a Delta table for supportability.
 *
 * <h3>Why before transformation?</h3>
 * Records that fail parsing, schema validation, or DLQ routing are still
 * persisted here, making it possible to replay or inspect any message that
 * ever reached the service.
 *
 * <h3>Storage</h3>
 * Writes to {@code {spark.output-base-path}/__raw_messages__} as a Delta table.
 * The flush runs on the shared {@link ThreadPoolTaskScheduler} (platform threads)
 * to avoid virtual-thread carrier pinning inside Spark's {@code synchronized} blocks.
 *
 * <h3>Thread safety</h3>
 * {@link #ingest} is called from virtual Kafka listener threads and is lock-free
 * (CAS-based {@link ConcurrentLinkedQueue}).  {@link #flush} is called from a
 * single scheduler thread, draining the queue with a non-blocking poll loop.
 */
@Component
@Slf4j
public class RawMessageStore {

    public static final String RAW_TABLE = "__raw_messages__";

    private final SparkSessionManager sparkSessionManager;
    private final SatsProperties satsProperties;
    private final ObjectMapper objectMapper;
    private final ThreadPoolTaskScheduler taskScheduler;

    @Value("${sats.raw-store.enabled:true}")
    private boolean enabled;

    @Value("${sats.raw-store.flush-interval-ms:10000}")
    private long flushIntervalMs;

    private final ConcurrentLinkedQueue<RawMessage> buffer = new ConcurrentLinkedQueue<>();

    public RawMessageStore(
            SparkSessionManager sparkSessionManager,
            SatsProperties satsProperties,
            ObjectMapper objectMapper,
            ThreadPoolTaskScheduler taskScheduler
    ) {
        this.sparkSessionManager = sparkSessionManager;
        this.satsProperties = satsProperties;
        this.objectMapper = objectMapper;
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    void scheduleFlush() {
        if (!enabled) {
            log.info("RawMessageStore is disabled — raw messages will not be persisted");
            return;
        }
        taskScheduler.scheduleWithFixedDelay(this::flush, Duration.ofMillis(flushIntervalMs));
        log.info("RawMessageStore flush scheduled every {}ms → {}/{}",
                flushIntervalMs, satsProperties.spark().outputBasePath(), RAW_TABLE);
    }

    /**
     * Enqueues the raw Kafka record for the next scheduled flush.
     * Lock-free; safe to call from any thread.
     */
    public void ingest(ConsumerRecord<String, byte[]> record, MessageType messageType) {
        if (!enabled) return;
        buffer.offer(new RawMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                messageType,
                Base64.getEncoder().encodeToString(record.value()),
                Instant.now()
        ));
    }

    // ── Flush (runs on platform scheduler thread) ─────────────────────────────

    void flush() {
        if (buffer.isEmpty()) return;

        List<RawMessage> batch = drain();
        log.debug("Flushing {} raw message(s) to {}", batch.size(), RAW_TABLE);

        String outputPath = satsProperties.spark().outputBasePath() + "/" + RAW_TABLE;
        var spark = sparkSessionManager.getSession();

        try {
            List<String> jsonRows = batch.stream()
                    .map(msg -> {
                        try {
                            return objectMapper.writeValueAsString(msg);
                        } catch (Exception e) {
                            throw new IllegalStateException("Failed to serialise RawMessage", e);
                        }
                    })
                    .collect(Collectors.toList());

            spark.read()
                    .json(spark.createDataset(jsonRows, Encoders.STRING()))
                    .write()
                    .format("delta")
                    .mode("append")
                    .save(outputPath);

            log.info("RawMessageStore flushed {} record(s) → {}", batch.size(), outputPath);
        } catch (Exception e) {
            log.error("RawMessageStore flush failed for {} record(s): {}", batch.size(), e.getMessage(), e);
            // Re-queue on failure so records are not lost
            buffer.addAll(batch);
        }
    }

    private List<RawMessage> drain() {
        var batch = new ArrayList<RawMessage>();
        RawMessage msg;
        while ((msg = buffer.poll()) != null) {
            batch.add(msg);
        }
        return batch;
    }
}
