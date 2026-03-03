package com.sats.domain.model;

import com.sats.domain.enums.DataType;

/**
 * Captures a single field that failed type validation.
 */
public record RescueEntry(
        String fieldName,
        Object originalValue,
        DataType expectedType,
        String failureReason
) {}
