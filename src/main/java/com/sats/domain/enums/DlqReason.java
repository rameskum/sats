package com.sats.domain.enums;

/**
 * Failure categories used as the {@code reason} header on DLQ messages.
 */
public enum DlqReason {
    PARSE_FAILURE,
    SCHEMA_MISMATCH,
    EXCESSIVE_RESCUE,
    WRITE_FAILURE
}
