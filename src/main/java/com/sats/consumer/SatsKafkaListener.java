package com.sats.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sats.batch.PerDatasetBatchManager;
import com.sats.dlq.DlqProducer;
import com.sats.domain.enums.DlqReason;
import com.sats.domain.model.SchemaDefinition;
import com.sats.registry.SchemaRegistryClient;
import com.sats.service.TransformationEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Processes batches of Kafka records: deserialises, transforms, and accumulates.
 * Implements {@link BatchAcknowledgingMessageListener} so the container can be
 * created and managed programmatically by {@code TopicSubscriptionManager},
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

    @Override
    public void onMessage(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) {
        log.debug("Received batch of {} records", records.size());

        for (var record : records) {
            processRecord(record);
        }

        ack.acknowledge();
    }

    private void processRecord(ConsumerRecord<String, byte[]> record) {
        Map<String, Object> rawMessage;
        try {
            rawMessage = objectMapper.readValue(record.value(), MAP_TYPE);
        } catch (Exception e) {
            log.error("Parse failure for {}:{}:{} — routing to DLQ",
                    record.topic(), record.partition(), record.offset(), e);
            dlqProducer.send(record, DlqReason.PARSE_FAILURE, e.getMessage());
            return;
        }

        // Resolve datasetId from topic (simple mapping; extend as needed)
        String datasetId = record.topic();

        SchemaDefinition schema = schemaRegistryClient.getSchema(datasetId)
                .orElse(null);

        if (schema == null) {
            log.error("No schema found for dataset {} — routing to DLQ", datasetId);
            dlqProducer.send(record, DlqReason.SCHEMA_MISMATCH, "Schema not found for dataset: " + datasetId);
            return;
        }

        var transformed = transformationEngine.transform(
                rawMessage, schema,
                record.topic(), record.partition(), record.offset()
        );

        if (transformed == null) {
            dlqProducer.send(record, DlqReason.EXCESSIVE_RESCUE, "Rescue threshold exceeded");
            return;
        }

        batchManager.accumulate(datasetId, transformed, record.value().length);
    }
}
