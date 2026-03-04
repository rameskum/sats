package com.sats.domain.model;

import com.sats.domain.enums.TargetType;

import java.util.List;

/**
 * Immutable snapshot of a flushed batch, handed off from the
 * {@code BatchAccumulator} to the platform-thread write pool.
 * {@code targetFormat} is resolved per-dataset from the schema registry.
 */
public record BatchPayload(
        String datasetId,
        List<TransformedRecord> records,
        long batchSizeBytes,
        String batchId,
        TargetType targetFormat
) {}
