package com.sats.batch;

import com.sats.config.BatchProperties;
import com.sats.domain.enums.TargetType;
import com.sats.domain.model.SchemaDefinition;
import com.sats.domain.model.TransformedRecord;
import com.sats.registry.SchemaRegistryClient;
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
 * The target write format is resolved per-dataset from the schema registry when
 * an accumulator is first created.  Falls back to {@link TargetType#DELTA} if
 * the registry is unavailable or returns no schema for the dataset.
 */
@Component
@Slf4j
public class PerDatasetBatchManager {

    private final BatchProperties batchProperties;
    private final WritePoolExecutor writePoolExecutor;
    private final SchemaRegistryClient schemaRegistryClient;
    private final ConcurrentMap<String, BatchAccumulator> accumulators = new ConcurrentHashMap<>();

    public PerDatasetBatchManager(BatchProperties batchProperties,
                                  WritePoolExecutor writePoolExecutor,
                                  SchemaRegistryClient schemaRegistryClient) {
        this.batchProperties = batchProperties;
        this.writePoolExecutor = writePoolExecutor;
        this.schemaRegistryClient = schemaRegistryClient;
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
        var targetFormat = schemaRegistryClient.getSchema(datasetId)
                .map(SchemaDefinition::targetFormat)
                .orElseGet(() -> {
                    log.warn("No schema found for dataset '{}'; defaulting to DELTA format", datasetId);
                    return TargetType.DELTA;
                });

        log.info("Creating batch accumulator for dataset {} (targetFormat={})", datasetId, targetFormat);
        return new BatchAccumulator(
                datasetId,
                batchProperties.maxRecords(),
                parseBytes(batchProperties.byteTarget()),
                batchProperties.maxTimer(),
                targetFormat
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
