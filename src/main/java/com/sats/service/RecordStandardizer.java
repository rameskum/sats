package com.sats.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sats.domain.model.TransformedRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Wraps a {@link TransformedRecord} into the flat output envelope described in Section 5.
 * Serialises sidecar maps as JSON strings and prepends Kafka metadata fields with
 * an underscore prefix.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RecordStandardizer {

    private final ObjectMapper objectMapper;

    public Map<String, Object> standardize(TransformedRecord record) {
        var output = new LinkedHashMap<String, Object>();

        // Business-domain fields
        output.putAll(record.schemaAlignedFields());

        // Sidecar fields (JSON-serialised)
        output.put("_rescue_data", toJson(record.rescueData()));
        output.put("_additional_columns", toJson(record.additionalColumns()));

        // Kafka metadata
        output.put("_kafka_topic", record.kafkaTopic());
        output.put("_kafka_partition", record.kafkaPartition());
        output.put("_kafka_offset", record.kafkaOffset());
        output.put("_ingestion_timestamp", record.ingestionTimestamp().toString());

        return output;
    }

    private String toJson(Map<String, Object> map) {
        if (map == null || map.isEmpty()) return "{}";
        try {
            return objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialise sidecar map: {}", e.getMessage());
            return "{}";
        }
    }
}
