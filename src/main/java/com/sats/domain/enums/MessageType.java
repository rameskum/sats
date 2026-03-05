package com.sats.domain.enums;

/**
 * Classifies the wire format of a Kafka message, determining how the
 * consumer decodes the payload and resolves the target dataset.
 *
 * <ul>
 *   <li>{@link #DATA_RECORD} — Protobuf {@code DataRecordEnvelope}; the dataset ID
 *       is embedded in the envelope. The inner payload bytes may be compressed.</li>
 *   <li>{@link #DATA_LOAD} — Protobuf {@code DataLoadEnvelope}; carries an additional
 *       {@code batchLoadId} correlating all records in a single load operation.</li>
 *   <li>{@link #PLAIN_TEXT} — Raw UTF-8 / JSON. The dataset ID is specified in
 *       the topic configuration, not in the message itself.</li>
 * </ul>
 */
public enum MessageType {
    DATA_RECORD,
    DATA_LOAD,
    PLAIN_TEXT
}
