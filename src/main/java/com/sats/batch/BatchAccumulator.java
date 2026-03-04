package com.sats.batch;

import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;
import com.sats.domain.model.TransformedRecord;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-dataset buffer with three-trigger flush policy (Section 8.2).
 * Uses {@link ReentrantLock} instead of {@code synchronized} to avoid
 * virtual-thread carrier pinning.
 */
@Slf4j
public class BatchAccumulator {

    private final String datasetId;
    private final int maxRecords;
    private final long maxBytes;
    private final Duration maxTimer;
    private final TargetType targetFormat;

    private final ReentrantLock lock = new ReentrantLock();
    private List<TransformedRecord> buffer;
    private long currentBytes;
    private Instant windowStart;

    public BatchAccumulator(String datasetId, int maxRecords, long maxBytes, Duration maxTimer,
                            TargetType targetFormat) {
        this.datasetId = datasetId;
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
        this.maxTimer = maxTimer;
        this.targetFormat = targetFormat;
        this.buffer = new ArrayList<>();
        this.currentBytes = 0;
        this.windowStart = Instant.now();
    }

    /**
     * Adds a record and returns a flush payload if any threshold is breached,
     * or {@code null} if the batch is still accumulating.
     */
    public BatchPayload add(TransformedRecord record, long estimatedBytes) {
        lock.lock();
        try {
            buffer.add(record);
            currentBytes += estimatedBytes;

            if (shouldFlush()) {
                return snapshot();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Timer-driven flush: returns a payload if any records are buffered, else null.
     */
    public BatchPayload timerFlush() {
        lock.lock();
        try {
            if (buffer.isEmpty()) return null;
            if (Instant.now().isAfter(windowStart.plus(maxTimer))) {
                return snapshot();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /** Zero-copy buffer swap: captures the current buffer and resets state. */
    private BatchPayload snapshot() {
        var flushed = buffer;
        var flushedBytes = currentBytes;

        // Reset — new list avoids copying
        buffer = new ArrayList<>();
        currentBytes = 0;
        windowStart = Instant.now();

        log.debug("Flushing batch for dataset {}: {} records, {} bytes",
                datasetId, flushed.size(), flushedBytes);

        return new BatchPayload(
                datasetId,
                List.copyOf(flushed),
                flushedBytes,
                UUID.randomUUID().toString(),
                targetFormat
        );
    }

    private boolean shouldFlush() {
        return buffer.size() >= maxRecords
                || currentBytes >= maxBytes
                || Instant.now().isAfter(windowStart.plus(maxTimer));
    }

    public int bufferSize() { return buffer.size(); }
    public long bufferBytes() { return currentBytes; }
}
