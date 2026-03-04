package com.sats.threading;

import com.sats.config.RetryProperties;
import com.sats.dlq.DlqProducer;
import com.sats.domain.model.BatchPayload;
import com.sats.writer.WriteProvider;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Submits flushed batches to the bounded platform-thread write pool (Section 9.5).
 * Failed writes are retried with exponential backoff; batches that exhaust all
 * attempts are routed to the DLQ via {@link DlqProducer#sendBatchFailure}.
 */
@Component
@Slf4j
public class WritePoolExecutor {

    private final ThreadPoolExecutor writePool;
    private final WriteProvider writeProvider;
    private final RetryProperties retryProperties;
    private final DlqProducer dlqProducer;

    public WritePoolExecutor(
            ThreadPoolExecutor writePoolExecutor,
            WriteProvider writeProvider,
            RetryProperties retryProperties,
            DlqProducer dlqProducer,
            MeterRegistry meterRegistry
    ) {
        this.writePool = writePoolExecutor;
        this.writeProvider = writeProvider;
        this.retryProperties = retryProperties;
        this.dlqProducer = dlqProducer;

        // Observability (Section 7.3)
        meterRegistry.gauge("sats.write.pool.active", writePool, ThreadPoolExecutor::getActiveCount);
        meterRegistry.gauge("sats.write.pool.queue.depth", writePool, e -> e.getQueue().size());
    }

    /**
     * Submits a batch for async write on the platform-thread pool.
     * If the pool and queue are both saturated, {@code CallerRunsPolicy} executes
     * the write on the calling (virtual) thread — providing natural backpressure.
     * <p>
     * On write failure the task is retried up to {@code sats.retry.max-attempts} times
     * with exponential backoff starting at {@code sats.retry.initial-backoff}.
     * After all attempts fail, every record in the batch is routed to the DLQ.
     */
    public void submit(BatchPayload payload) {
        writePool.execute(() -> {
            int maxAttempts = retryProperties.maxAttempts();
            long backoffMs  = retryProperties.initialBackoff().toMillis();

            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    writeProvider.write(payload);
                    return;                              // success — exit retry loop
                } catch (Exception e) {
                    if (attempt == maxAttempts) {
                        log.error("Write failed after {} attempt(s) for batch {} (dataset={}): {}",
                                maxAttempts, payload.batchId(), payload.datasetId(), e.getMessage(), e);
                        dlqProducer.sendBatchFailure(payload, e.getMessage());
                        return;
                    }
                    log.warn("Write attempt {}/{} failed for batch {} (dataset={}), retrying in {}ms: {}",
                            attempt, maxAttempts, payload.batchId(), payload.datasetId(),
                            backoffMs, e.getMessage());
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.warn("Retry interrupted for batch {}, routing to DLQ", payload.batchId());
                        dlqProducer.sendBatchFailure(payload, "Interrupted during retry: " + e.getMessage());
                        return;
                    }
                    backoffMs = (long) (backoffMs * retryProperties.backoffMultiplier());
                }
            }
        });
    }
}
