package com.sats.domain.model;

import lombok.Builder;

import java.time.Instant;
import java.util.Map;

/**
 * The output envelope emitted by the TransformationEngine (Section 5).
 * Carries the validated payload alongside rescue, extra, and Kafka metadata.
 * {@code batchLoadId} is non-null only for {@code DATA_LOAD} messages and
 * correlates all records belonging to the same bulk-load operation.
 */
@Builder
public record TransformedRecord(
        Map<String, Object> schemaAlignedFields,
        Map<String, Object> rescueData,
        Map<String, Object> additionalColumns,
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset,
        String batchLoadId,       // null for DATA_RECORD and PLAIN_TEXT
        Instant ingestionTimestamp
) {}
