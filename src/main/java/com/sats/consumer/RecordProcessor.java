package com.sats.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sats.batch.PerDatasetBatchManager;
import com.sats.config.SatsProperties;
import com.sats.dlq.DlqProducer;
import com.sats.domain.enums.DlqReason;
import com.sats.domain.enums.MessageType;
import com.sats.proto.Compression;
import com.sats.proto.DataLoadEnvelope;
import com.sats.proto.DataRecordEnvelope;
import com.sats.registry.SchemaRegistryClient;
import com.sats.service.TransformationEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Core record processing pipeline, shared by the live Kafka listener and the
 * replay path.
 *
 * <p>Accepts raw bytes + minimal metadata, handles all three message types, and
 * routes failures to the DLQ.  Callers (live or replay) are responsible for
 * raw-message persistence and offset management.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RecordProcessor {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;
    private final SchemaRegistryClient schemaRegistryClient;
    private final TransformationEngine transformationEngine;
    private final PerDatasetBatchManager batchManager;
    private final DlqProducer dlqProducer;
    private final TopicRegistry topicRegistry;

    /**
     * Processes one record through the full pipeline: decode → decompress (if needed) →
     * JSON parse → schema lookup → transform → accumulate.
     *
     * <p>On any failure the record is routed to the DLQ and the method returns normally
     * (non-throwing), so batch processing can continue with remaining records.
     *
     * @param rawBytes    the raw {@code record.value()} bytes (or base64-decoded bytes for replay)
     * @param messageType the message type resolved from the topic descriptor
     * @param topic       source Kafka topic
     * @param partition   source Kafka partition
     * @param offset      source Kafka offset
     * @param key         source Kafka record key (may be null for replay)
     */
    public void process(byte[] rawBytes, MessageType messageType,
                        String topic, int partition, long offset, String key) {
        switch (messageType) {
            case DATA_RECORD -> processDataRecord(rawBytes, topic, partition, offset, key);
            case DATA_LOAD   -> processDataLoad(rawBytes, topic, partition, offset, key);
            case PLAIN_TEXT  -> processPlainText(rawBytes, topic, partition, offset, key);
        }
    }

    // ── DATA_RECORD ──────────────────────────────────────────────────────────

    private void processDataRecord(byte[] rawBytes, String topic, int partition, long offset, String key) {
        DataRecordEnvelope envelope;
        try {
            envelope = DataRecordEnvelope.parseFrom(rawBytes);
        } catch (Exception e) {
            log.error("Protobuf parse failure (DATA_RECORD) at {}:{}:{}", topic, partition, offset, e);
            dlqProducer.send(topic, partition, offset, key, rawBytes, DlqReason.PARSE_FAILURE, e.getMessage());
            return;
        }

        byte[] payload;
        try {
            payload = decompress(envelope.getPayload().toByteArray(), envelope.getCompression());
        } catch (Exception e) {
            log.error("Decompression failure (DATA_RECORD) at {}:{}:{}", topic, partition, offset, e);
            dlqProducer.send(topic, partition, offset, key, rawBytes,
                    DlqReason.PARSE_FAILURE, "Decompression failed: " + e.getMessage());
            return;
        }

        transformAndAccumulate(payload, envelope.getDatasetId(), null, topic, partition, offset, key, rawBytes);
    }

    // ── DATA_LOAD ────────────────────────────────────────────────────────────

    private void processDataLoad(byte[] rawBytes, String topic, int partition, long offset, String key) {
        DataLoadEnvelope envelope;
        try {
            envelope = DataLoadEnvelope.parseFrom(rawBytes);
        } catch (Exception e) {
            log.error("Protobuf parse failure (DATA_LOAD) at {}:{}:{}", topic, partition, offset, e);
            dlqProducer.send(topic, partition, offset, key, rawBytes, DlqReason.PARSE_FAILURE, e.getMessage());
            return;
        }

        byte[] payload;
        try {
            payload = decompress(envelope.getPayload().toByteArray(), envelope.getCompression());
        } catch (Exception e) {
            log.error("Decompression failure (DATA_LOAD) at {}:{}:{}", topic, partition, offset, e);
            dlqProducer.send(topic, partition, offset, key, rawBytes,
                    DlqReason.PARSE_FAILURE, "Decompression failed: " + e.getMessage());
            return;
        }

        transformAndAccumulate(payload, envelope.getDatasetId(), envelope.getBatchLoadId(),
                topic, partition, offset, key, rawBytes);
    }

    // ── PLAIN_TEXT ───────────────────────────────────────────────────────────

    private void processPlainText(byte[] rawBytes, String topic, int partition, long offset, String key) {
        // datasetId is defined in the topic config; fall back to the topic name itself
        String datasetId = topicRegistry.lookup(topic)
                .map(SatsProperties.ConsumerConfig.TopicDescriptor::datasetId)
                .orElse(topic);
        transformAndAccumulate(rawBytes, datasetId, null, topic, partition, offset, key, rawBytes);
    }

    // ── Shared transform + accumulate path ───────────────────────────────────

    private void transformAndAccumulate(
            byte[] jsonBytes, String datasetId, String batchLoadId,
            String topic, int partition, long offset, String key, byte[] originalBytes
    ) {
        Map<String, Object> rawMessage;
        try {
            rawMessage = objectMapper.readValue(jsonBytes, MAP_TYPE);
        } catch (Exception e) {
            log.error("JSON parse failure for dataset '{}' at {}:{}:{}", datasetId, topic, partition, offset, e);
            dlqProducer.send(topic, partition, offset, key, originalBytes, DlqReason.PARSE_FAILURE, e.getMessage());
            return;
        }

        var schema = schemaRegistryClient.getSchema(datasetId).orElse(null);
        if (schema == null) {
            log.error("No schema found for dataset '{}' at {}:{}:{} — routing to DLQ",
                    datasetId, topic, partition, offset);
            dlqProducer.send(topic, partition, offset, key, originalBytes,
                    DlqReason.SCHEMA_MISMATCH, "Schema not found for dataset: " + datasetId);
            return;
        }

        var transformed = transformationEngine.transform(
                rawMessage, schema, topic, partition, offset, batchLoadId);

        if (transformed == null) {
            dlqProducer.send(topic, partition, offset, key, originalBytes,
                    DlqReason.EXCESSIVE_RESCUE, "Rescue threshold exceeded");
            return;
        }

        batchManager.accumulate(datasetId, transformed, jsonBytes.length);
    }

    // ── Decompression ────────────────────────────────────────────────────────

    private byte[] decompress(byte[] data, Compression compression) throws IOException {
        return switch (compression) {
            case GZIP -> {
                try (var gz = new GZIPInputStream(new ByteArrayInputStream(data))) {
                    yield gz.readAllBytes();
                }
            }
            case SNAPPY -> throw new UnsupportedOperationException(
                    "SNAPPY decompression requires the snappy-java dependency");
            default -> data;
        };
    }
}
