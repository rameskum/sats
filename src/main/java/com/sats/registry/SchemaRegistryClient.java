package com.sats.registry;

import com.sats.config.SatsProperties;
import com.sats.domain.model.SchemaDefinition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Fetches and caches target schemas per dataset.
 * <p>
 * The cache uses TTL-based invalidation (Section 3.2). If more than 3 consecutive
 * batches encounter unknown fields, an immediate refresh is triggered.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SchemaRegistryClient {

    private final SatsProperties satsProperties;
    private final ConcurrentMap<String, SchemaDefinition> cache = new ConcurrentHashMap<>();

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
        // TODO: Fetch from satsProperties.schema().registryUrl()
        //       Normalize JSON Schema / StructType / Protobuf into SchemaDefinition
        //       cache.put(datasetId, newDefinition);
        return Optional.empty();
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
}
