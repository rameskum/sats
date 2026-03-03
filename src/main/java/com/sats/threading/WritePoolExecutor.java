package com.sats.threading;

import com.sats.domain.model.BatchPayload;
import com.sats.writer.WriteProvider;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Submits flushed batches to the bounded platform-thread write pool (Section 9.5).
 * Exposes pool metrics via Micrometer.
 */
@Component
@Slf4j
public class WritePoolExecutor {

    private final ThreadPoolExecutor writePool;
    private final WriteProvider writeProvider;

    public WritePoolExecutor(
            ThreadPoolExecutor writePoolExecutor,
            WriteProvider writeProvider,
            MeterRegistry meterRegistry
    ) {
        this.writePool = writePoolExecutor;
        this.writeProvider = writeProvider;

        // Observability (Section 7.3)
        meterRegistry.gauge("sats.write.pool.active", writePool, ThreadPoolExecutor::getActiveCount);
        meterRegistry.gauge("sats.write.pool.queue.depth", writePool, e -> e.getQueue().size());
    }

    /**
     * Submits a batch for async write on the platform-thread pool.
     * If the pool and queue are both saturated, {@code CallerRunsPolicy} executes
     * the write on the calling (virtual) thread — providing natural backpressure.
     */
    public void submit(BatchPayload payload) {
        writePool.execute(() -> {
            try {
                writeProvider.write(payload);
            } catch (Exception e) {
                log.error("Write failed for batch {} (dataset {}): {}",
                        payload.batchId(), payload.datasetId(), e.getMessage(), e);
                // TODO: Retry with backoff or route batch metadata to DLQ
            }
        });
    }
}
