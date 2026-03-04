package com.sats.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sats.config.SatsProperties;
import com.sats.domain.enums.DataType;
import com.sats.domain.model.FieldSpec;
import com.sats.domain.model.SchemaDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Fetches and caches target schemas per dataset.
 *
 * <h3>Cache</h3>
 * TTL-based invalidation: entries older than {@code sats.schema.cache-ttl} are
 * re-fetched on next access.  {@link #evict(String)} triggers an immediate
 * removal; {@link #refreshSchema(String)} forces a synchronous HTTP re-fetch.
 *
 * <h3>Registry contract</h3>
 * {@code GET {registryUrl}/{datasetId}} must return:
 * <pre>
 * {
 *   "datasetId": "orders",
 *   "fields": [
 *     { "fieldName": "orderId",    "targetType": "STRING",    "nullable": false },
 *     { "fieldName": "amount",     "targetType": "DOUBLE",    "nullable": true  },
 *     { "fieldName": "createdAt",  "targetType": "TIMESTAMP", "nullable": false,
 *       "nestedPath": ["meta", "time"] }
 *   ]
 * }
 * </pre>
 */
@Component
@Slf4j
public class SchemaRegistryClient {

    private final SatsProperties satsProperties;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, SchemaDefinition> cache = new ConcurrentHashMap<>();
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public SchemaRegistryClient(SatsProperties satsProperties, ObjectMapper objectMapper) {
        this.satsProperties = satsProperties;
        this.objectMapper = objectMapper;
    }

    /**
     * Returns the cached schema for the given dataset, refreshing if the TTL has expired.
     */
    public Optional<SchemaDefinition> getSchema(String datasetId) {
        var cached = cache.get(datasetId);
        if (cached != null && !isExpired(cached)) {
            return Optional.of(cached);
        }
        return refreshSchema(datasetId);
    }

    /**
     * Forces an immediate cache refresh for the given dataset.
     */
    public Optional<SchemaDefinition> refreshSchema(String datasetId) {
        log.info("Refreshing schema for dataset: {}", datasetId);
        String url = satsProperties.schema().registryUrl() + "/" + datasetId;

        try {
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 404) {
                log.warn("Schema not found for dataset '{}' (HTTP 404)", datasetId);
                return Optional.empty();
            }
            if (response.statusCode() != 200) {
                log.error("Schema registry returned HTTP {} for dataset '{}'",
                        response.statusCode(), datasetId);
                return Optional.empty();
            }

            var dto = objectMapper.readValue(response.body(), SchemaRegistryResponse.class);
            Map<String, FieldSpec> fields = dto.fields().stream()
                    .collect(Collectors.toMap(
                            FieldDto::fieldName,
                            f -> new FieldSpec(
                                    f.fieldName(),
                                    f.targetType(),
                                    f.nullable(),
                                    f.nestedPath() != null ? f.nestedPath() : new String[0]
                            )
                    ));

            var definition = new SchemaDefinition(datasetId, fields, Instant.now());
            cache.put(datasetId, definition);
            log.info("Schema cached for dataset '{}': {} field(s)", datasetId, fields.size());
            return Optional.of(definition);

        } catch (Exception e) {
            log.error("Failed to fetch schema for dataset '{}' from {}: {}",
                    datasetId, url, e.getMessage(), e);
            return Optional.empty();
        }
    }

    public void evict(String datasetId) {
        cache.remove(datasetId);
        log.debug("Evicted schema cache for dataset: {}", datasetId);
    }

    private boolean isExpired(SchemaDefinition definition) {
        return Instant.now().isAfter(
                definition.fetchedAt().plus(satsProperties.schema().cacheTtl())
        );
    }

    // ── Registry response DTOs ────────────────────────────────────────

    private record SchemaRegistryResponse(String datasetId, List<FieldDto> fields) {}

    private record FieldDto(
            String fieldName,
            DataType targetType,
            boolean nullable,
            String[] nestedPath
    ) {}
}
