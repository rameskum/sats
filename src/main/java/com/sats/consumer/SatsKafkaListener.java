package com.sats.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sats.batch.PerDatasetBatchManager;
import com.sats.config.SatsProperties;
import com.sats.dlq.DlqProducer;
import com.sats.domain.enums.DlqReason;
import com.sats.domain.enums.MessageType;
import com.sats.domain.model.SchemaDefinition;
import com.sats.proto.DataLoadEnvelope;
import com.sats.proto.DataRecordEnvelope;
import com.sats.registry.SchemaRegistryClient;
import com.sats.service.TransformationEngine;
import com.sats.store.RawMessageStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Processes batches of Kafka records: decodes by message type, transforms, and accumulates.
 *
 * <h3>Message routing</h3>
 * <ul>
 *   <li>{@link MessageType#DATA_RECORD} — unwraps a {@code DataRecordEnvelope} (Protobuf),
 *       resolves {@code datasetId} from the envelope, decompresses if needed, then
 *       parses the inner payload as JSON and transforms it.</li>
 *   <li>{@link MessageType#DATA_LOAD} — same as DATA_RECORD but also extracts
 *       {@code batchLoadId} from the {@code DataLoadEnvelope} and stamps it on the
 *       resulting {@link com.sats.domain.model.TransformedRecord}.</li>
 *   <li>{@link MessageType#PLAIN_TEXT} — reads the raw Kafka value as UTF-8 JSON.
 *       The {@code datasetId} is taken from the topic descriptor in the config.</li>
 * </ul>
 *
 * <p>Implements {@link BatchAcknowledgingMessageListener} so the container can be
 * created and managed programmatically by {@link TopicSubscriptionManager},
 * enabling runtime topic reload without restarting the application.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SatsKafkaListener implements BatchAcknowledgingMessageListener<String, byte[]> {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;
    private final SchemaRegistryClient schemaRegistryClient;
    private final TransformationEngine transformationEngine;
    private final PerDatasetBatchManager batchManager;
    private final DlqProducer dlqProducer;
    private final TopicRegistry topicRegistry;
    private final RawMessageStore rawMessageStore;

    @Override
    public void onMessage(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) {
        log.debug("Received batch of {} records", records.size());
        for (var record : records) {
            processRecord(record);
        }
        ack.acknowledge();
    }

    // ── Dispatch ─────────────────────────────────────────────────────────────

    private void processRecord(ConsumerRecord<String, byte[]> record) {
        var descriptor = topicRegistry.lookup(record.topic())
                .orElseGet(() -> {
                    log.warn("No descriptor for topic '{}' — treating as PLAIN_TEXT with topic as datasetId",
                            record.topic());
                    return new SatsProperties.ConsumerConfig.TopicDescriptor(
                            record.topic(), MessageType.PLAIN_TEXT, record.topic());
                });

        // Persist the raw message before any transformation so even
        // parse/schema failures are captured for replay and debugging.
        rawMessageStore.ingest(record, descriptor.type());

        switch (descriptor.type()) {
            case DATA_RECORD -> processDataRecord(record);
            case DATA_LOAD   -> processDataLoad(record);
            case PLAIN_TEXT  -> processPlainText(record, descriptor.datasetId());
        }
    }

    // ── DATA_RECORD ──────────────────────────────────────────────────────────

    private void processDataRecord(ConsumerRecord<String, byte[]> record) {
        DataRecordEnvelope envelope;
        try {
            envelope = DataRecordEnvelope.parseFrom(record.value());
        } catch (Exception e) {
            log.error("Protobuf parse failure (DATA_RECORD) at {}:{}:{}", record.topic(),
                    record.partition(), record.offset(), e);
            dlqProducer.send(record, DlqReason.PARSE_FAILURE, e.getMessage());
            return;
        }

        byte[] rawBytes;
        try {
            rawBytes = decompress(envelope.getPayload().toByteArray(), envelope.getCompression());
        } catch (Exception e) {
            log.error("Decompression failure (DATA_RECORD) at {}:{}:{}", record.topic(),
                    record.partition(), record.offset(), e);
            dlqProducer.send(record, DlqReason.PARSE_FAILURE, "Decompression failed: " + e.getMessage());
            return;
        }

        transformAndAccumulate(record, rawBytes, envelope.getDatasetId(), null);
    }

    // ── DATA_LOAD ────────────────────────────────────────────────────────────

    private void processDataLoad(ConsumerRecord<String, byte[]> record) {
        DataLoadEnvelope envelope;
        try {
            envelope = DataLoadEnvelope.parseFrom(record.value());
        } catch (Exception e) {
            log.error("Protobuf parse failure (DATA_LOAD) at {}:{}:{}", record.topic(),
                    record.partition(), record.offset(), e);
            dlqProducer.send(record, DlqReason.PARSE_FAILURE, e.getMessage());
            return;
        }

        byte[] rawBytes;
        try {
            rawBytes = decompress(envelope.getPayload().toByteArray(), envelope.getCompression());
        } catch (Exception e) {
            log.error("Decompression failure (DATA_LOAD) at {}:{}:{}", record.topic(),
                    record.partition(), record.offset(), e);
            dlqProducer.send(record, DlqReason.PARSE_FAILURE, "Decompression failed: " + e.getMessage());
            return;
        }

        transformAndAccumulate(record, rawBytes, envelope.getDatasetId(), envelope.getBatchLoadId());
    }

    // ── PLAIN_TEXT ───────────────────────────────────────────────────────────

    private void processPlainText(ConsumerRecord<String, byte[]> record, String datasetId) {
        transformAndAccumulate(record, record.value(), datasetId, null);
    }

    // ── Shared transform + accumulate path ───────────────────────────────────

    private void transformAndAccumulate(
            ConsumerRecord<String, byte[]> record,
            byte[] rawBytes,
            String datasetId,
            String batchLoadId
    ) {
        Map<String, Object> rawMessage;
        try {
            rawMessage = objectMapper.readValue(rawBytes, MAP_TYPE);
        } catch (Exception e) {
            log.error("JSON parse failure for dataset '{}' at {}:{}:{}",
                    datasetId, record.topic(), record.partition(), record.offset(), e);
            dlqProducer.send(record, DlqReason.PARSE_FAILURE, e.getMessage());
            return;
        }

        SchemaDefinition schema = schemaRegistryClient.getSchema(datasetId).orElse(null);
        if (schema == null) {
            log.error("No schema found for dataset '{}' — routing to DLQ", datasetId);
            dlqProducer.send(record, DlqReason.SCHEMA_MISMATCH,
                    "Schema not found for dataset: " + datasetId);
            return;
        }

        var transformed = transformationEngine.transform(
                rawMessage, schema,
                record.topic(), record.partition(), record.offset(),
                batchLoadId);

        if (transformed == null) {
            dlqProducer.send(record, DlqReason.EXCESSIVE_RESCUE, "Rescue threshold exceeded");
            return;
        }

        batchManager.accumulate(datasetId, transformed, rawBytes.length);
    }

    // ── Decompression ────────────────────────────────────────────────────────

    private byte[] decompress(byte[] data, com.sats.proto.Compression compression) throws IOException {
        return switch (compression) {
            case GZIP -> {
                try (var gz = new GZIPInputStream(new ByteArrayInputStream(data))) {
                    yield gz.readAllBytes();
                }
            }
            case SNAPPY -> throw new UnsupportedOperationException(
                    "SNAPPY decompression requires the snappy-java dependency");
            default -> data; // NONE or unrecognised
        };
    }
}
