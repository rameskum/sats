package com.sats.domain.model;

import com.sats.domain.enums.MessageType;

import java.time.Instant;

/**
 * Verbatim capture of a single Kafka record before any transformation.
 * Written to the {@code __raw_messages__} Delta table for supportability:
 * debugging failed transformations, schema-change replay, and audit.
 *
 * <p>{@code rawValueB64} is the Base64-encoded raw {@code record.value()} bytes,
 * preserving the original wire format (Protobuf or plain text) exactly as received.
 */
public record RawMessage(
        String topic,
        int    partition,
        long   offset,
        String messageKey,
        MessageType messageType,
        String rawValueB64,
        Instant receivedAt
) {}
