package com.sats.batch;

import com.sats.config.BatchProperties;
import com.sats.domain.model.BatchPayload;
import com.sats.domain.model.TransformedRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Manages per-dataset {@link BatchAccumulator} instances (Section 8.2).
 * A scheduled timer periodically checks all accumulators for time-based flushes.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PerDatasetBatchManager {

    private final BatchProperties batchProperties;
    private final ConcurrentMap<String, BatchAccumulator> accumulators = new ConcurrentHashMap<>();

    /** Callback invoked when a batch is ready to be submitted to the write pool. */
    private Consumer<BatchPayload> flushCallback = payload ->
            log.warn("No flush callback registered — batch {} for dataset {} dropped",
                    payload.batchId(), payload.datasetId());

    public void registerFlushCallback(Consumer<BatchPayload> callback) {
        this.flushCallback = callback;
    }

    public void accumulate(String datasetId, TransformedRecord record, long estimatedBytes) {
        var accumulator = accumulators.computeIfAbsent(datasetId, this::createAccumulator);
        var payload = accumulator.add(record, estimatedBytes);
        if (payload != null) {
            flushCallback.accept(payload);
        }
    }

    @Scheduled(fixedDelayString = "${sats.batch.timer-check-interval-ms:5000}")
    public void timerFlushAll() {
        accumulators.forEach((datasetId, accumulator) -> {
            var payload = accumulator.timerFlush();
            if (payload != null) {
                flushCallback.accept(payload);
            }
        });
    }

    private BatchAccumulator createAccumulator(String datasetId) {
        log.info("Creating batch accumulator for dataset {}", datasetId);
        return new BatchAccumulator(
                datasetId,
                batchProperties.maxRecords(),
                parseBytes(batchProperties.byteTarget()),
                batchProperties.maxTimer()
        );
    }

    private long parseBytes(String size) {
        String upper = size.toUpperCase().trim();
        if (upper.endsWith("GB")) return Long.parseLong(upper.replace("GB", "").trim()) * 1024 * 1024 * 1024;
        if (upper.endsWith("MB")) return Long.parseLong(upper.replace("MB", "").trim()) * 1024 * 1024;
        if (upper.endsWith("KB")) return Long.parseLong(upper.replace("KB", "").trim()) * 1024;
        return Long.parseLong(upper);
    }
}
