package com.sats.service;

import com.sats.config.SatsProperties;
import com.sats.domain.model.FieldSpec;
import com.sats.domain.model.SchemaDefinition;
import com.sats.domain.model.TransformedRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Core transformation loop (Section 4). For each raw message:
 * <ol>
 *   <li>Identifies extra fields not in the target schema → {@code additional_columns}</li>
 *   <li>Validates and safe-casts expected fields → payload or {@code rescue_data}</li>
 *   <li>Wraps the result in a {@link TransformedRecord}</li>
 * </ol>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransformationEngine {

    private final SchemaValidator schemaValidator;
    private final SatsProperties satsProperties;

    /**
     * Transforms a single raw message map against the given schema definition.
     *
     * @return The standardised record, or {@code null} if the record should be DLQ'd
     *         (excessive rescue threshold breached).
     */
    public TransformedRecord transform(
            Map<String, Object> rawMessage,
            SchemaDefinition schema,
            String kafkaTopic,
            int kafkaPartition,
            long kafkaOffset
    ) {
        Map<String, Object> aligned = new HashMap<>();
        Map<String, Object> rescueData = new HashMap<>();
        Map<String, Object> extraColumns = new HashMap<>();

        // ── Step 1: Separate extra fields ──
        for (var entry : rawMessage.entrySet()) {
            if (!schema.fields().containsKey(entry.getKey())) {
                extraColumns.put(entry.getKey(), entry.getValue());
            }
        }

        // ── Step 2: Validate and cast schema-expected fields ──
        for (var fieldEntry : schema.fields().entrySet()) {
            String fieldName = fieldEntry.getKey();
            FieldSpec spec = fieldEntry.getValue();
            Object rawValue = rawMessage.get(fieldName);

            if (rawValue == null) {
                aligned.put(fieldName, null);
                if (!spec.nullable()) {
                    rescueData.put(fieldName, null);
                }
                continue;
            }

            schemaValidator.safeCast(rawValue, spec)
                    .ifPresentOrElse(
                            castValue -> aligned.put(fieldName, castValue),
                            () -> {
                                rescueData.put(fieldName, rawValue);
                                aligned.put(fieldName, null);
                            }
                    );
        }

        // ── Step 3: Excessive rescue check ──
        int totalFields = schema.fields().size();
        int rescuedCount = rescueData.size();
        double rescueRate = totalFields > 0 ? (double) rescuedCount / totalFields * 100 : 0;

        if (rescueRate > satsProperties.schema().rescueThresholdPercent()) {
            log.warn(
                    "Record from {}:{}:{} exceeded rescue threshold ({:.1f}% > {}%). Routing to DLQ.",
                    kafkaTopic, kafkaPartition, kafkaOffset,
                    rescueRate, satsProperties.schema().rescueThresholdPercent()
            );
            return null; // Caller routes to DLQ with EXCESSIVE_RESCUE reason
        }

        return TransformedRecord.builder()
                .schemaAlignedFields(Map.copyOf(aligned))
                .rescueData(Map.copyOf(rescueData))
                .additionalColumns(Map.copyOf(extraColumns))
                .kafkaTopic(kafkaTopic)
                .kafkaPartition(kafkaPartition)
                .kafkaOffset(kafkaOffset)
                .ingestionTimestamp(Instant.now())
                .build();
    }
}
