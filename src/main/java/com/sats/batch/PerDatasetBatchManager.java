package com.sats.batch;

import com.sats.config.BatchProperties;
import com.sats.domain.model.BatchPayload;
import com.sats.domain.model.TransformedRecord;
import com.sats.threading.WritePoolExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages per-dataset {@link BatchAccumulator} instances (Section 8.2).
 * A scheduled timer periodically checks all accumulators for time-based flushes.
 * <p>
 * When an accumulator crosses a flush threshold (record count, byte size, or
 * time window), the resulting {@link BatchPayload} is handed directly to
 * {@link WritePoolExecutor#submit(BatchPayload)}, which queues it on the
 * bounded platform-thread write pool.
 */
@Component
@Slf4j
public class PerDatasetBatchManager {

    private final BatchProperties batchProperties;
    private final WritePoolExecutor writePoolExecutor;
    private final ConcurrentMap<String, BatchAccumulator> accumulators = new ConcurrentHashMap<>();

    public PerDatasetBatchManager(BatchProperties batchProperties,
                                  WritePoolExecutor writePoolExecutor) {
        this.batchProperties = batchProperties;
        this.writePoolExecutor = writePoolExecutor;
    }

    public void accumulate(String datasetId, TransformedRecord record, long estimatedBytes) {
        var accumulator = accumulators.computeIfAbsent(datasetId, this::createAccumulator);
        var payload = accumulator.add(record, estimatedBytes);
        if (payload != null) {
            writePoolExecutor.submit(payload);
        }
    }

    @Scheduled(fixedDelayString = "${sats.batch.timer-check-interval-ms:5000}")
    public void timerFlushAll() {
        accumulators.forEach((datasetId, accumulator) -> {
            var payload = accumulator.timerFlush();
            if (payload != null) {
                writePoolExecutor.submit(payload);
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
