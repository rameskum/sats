package com.sats.replay;

import com.sats.domain.enums.MessageType;

/**
 * Parameters for a replay operation against the {@code __raw_messages__} Delta table.
 *
 * <ul>
 *   <li>{@code topic} — required; the Kafka topic to replay.</li>
 *   <li>{@code partition} — optional; {@code null} replays all partitions.</li>
 *   <li>{@code startOffset} — inclusive lower bound on the Kafka offset.</li>
 *   <li>{@code endOffset} — inclusive upper bound; {@code null} = no upper bound.</li>
 *   <li>{@code messageType} — optional filter; {@code null} = all types.</li>
 *   <li>{@code limit} — max records to process in one call (default 1 000, safety cap).</li>
 * </ul>
 */
public record ReplayRequest(
        String topic,
        Integer partition,
        long startOffset,
        Long endOffset,
        MessageType messageType,
        int limit
) {
    public ReplayRequest {
        if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic is required");
        if (startOffset < 0)                  throw new IllegalArgumentException("startOffset must be >= 0");
        if (limit <= 0)                       limit = 1_000;
    }
}
