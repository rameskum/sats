package com.sats.domain.model;

import java.util.List;

/**
 * Immutable snapshot of a flushed batch, handed off from the
 * {@code BatchAccumulator} to the platform-thread write pool.
 */
public record BatchPayload(
        String datasetId,
        List<TransformedRecord> records,
        long batchSizeBytes,
        String batchId
) {}
