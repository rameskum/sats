package com.sats.domain.model;

import com.sats.domain.enums.TargetType;

import java.time.Instant;
import java.util.Map;

/**
 * Cached schema for a single dataset.
 * The {@code fields} map is the primary lookup structure for Phase 2 transformation.
 * {@code targetFormat} defines the write format (Delta, Parquet, JSON) for this dataset.
 */
public record SchemaDefinition(
        String datasetId,
        Map<String, FieldSpec> fields,
        TargetType targetFormat,
        Instant fetchedAt
) {}
