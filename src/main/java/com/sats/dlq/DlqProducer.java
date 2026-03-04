package com.sats.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sats.config.SatsProperties;
import com.sats.domain.enums.DlqReason;
import com.sats.domain.model.BatchPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Publishes unparseable or failed records to the configured DLQ topic
 * with structured headers (Section 7.1).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DlqProducer {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final SatsProperties satsProperties;
    private final ObjectMapper objectMapper;

    public void send(ConsumerRecord<String, byte[]> original, DlqReason reason, String errorMessage) {
        var dlqRecord = new ProducerRecord<>(
                satsProperties.dlq().topic(),
                original.key(),
                original.value()
        );

        addHeader(dlqRecord, "reason", reason.name());
        addHeader(dlqRecord, "source_topic", original.topic());
        addHeader(dlqRecord, "source_partition", String.valueOf(original.partition()));
        addHeader(dlqRecord, "source_offset", String.valueOf(original.offset()));
        addHeader(dlqRecord, "error_message", truncate(errorMessage, 1024));
        addHeader(dlqRecord, "timestamp", Instant.now().toString());

        kafkaTemplate.send(dlqRecord);
        log.warn("Published to DLQ [{}]: topic={}, partition={}, offset={}, reason={}",
                satsProperties.dlq().topic(),
                original.topic(), original.partition(), original.offset(), reason);
    }

    /**
     * Routes all records in a failed {@link BatchPayload} to the DLQ.
     * Each record is serialized as JSON bytes; Kafka metadata from the
     * original {@code TransformedRecord} is preserved in headers.
     */
    public void sendBatchFailure(BatchPayload payload, String errorMessage) {
        String dlqTopic = satsProperties.dlq().topic();
        for (var record : payload.records()) {
            byte[] value;
            try {
                value = objectMapper.writeValueAsBytes(record.schemaAlignedFields());
            } catch (Exception ex) {
                log.warn("Could not serialize record for DLQ (batch={}, offset={}): {}",
                        payload.batchId(), record.kafkaOffset(), ex.getMessage());
                continue;
            }
            var dlqRecord = new ProducerRecord<String, byte[]>(dlqTopic, null, value);
            addHeader(dlqRecord, "reason", DlqReason.WRITE_FAILURE.name());
            addHeader(dlqRecord, "source_topic", record.kafkaTopic());
            addHeader(dlqRecord, "source_partition", String.valueOf(record.kafkaPartition()));
            addHeader(dlqRecord, "source_offset", String.valueOf(record.kafkaOffset()));
            addHeader(dlqRecord, "batch_id", payload.batchId());
            addHeader(dlqRecord, "dataset_id", payload.datasetId());
            addHeader(dlqRecord, "error_message", truncate(errorMessage, 1024));
            addHeader(dlqRecord, "timestamp", Instant.now().toString());
            kafkaTemplate.send(dlqRecord);
        }
        log.warn("Routed {} record(s) from batch {} (dataset={}) to DLQ after write failure",
                payload.records().size(), payload.batchId(), payload.datasetId());
    }

    private void addHeader(ProducerRecord<String, byte[]> record, String key, String value) {
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    private String truncate(String value, int maxLength) {
        if (value == null) return "";
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
