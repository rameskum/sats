package com.sats.domain.model;

import com.sats.domain.enums.DataType;

/**
 * Describes a single field in the target schema.
 * Used as the value type in the flattened schema lookup map.
 */
public record FieldSpec(
        String fieldName,
        DataType targetType,
        boolean nullable,
        String[] nestedPath
) {}
