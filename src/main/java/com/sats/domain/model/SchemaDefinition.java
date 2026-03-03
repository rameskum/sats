package com.sats.domain.model;

import java.time.Instant;
import java.util.Map;

/**
 * Cached schema for a single dataset.
 * The {@code fields} map is the primary lookup structure for Phase 2 transformation.
 */
public record SchemaDefinition(
        String datasetId,
        Map<String, FieldSpec> fields,
        Instant fetchedAt
) {}
