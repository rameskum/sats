package com.sats.consumer;

import com.sats.config.SatsProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Shared lookup table from Kafka topic name → {@link SatsProperties.ConsumerConfig.TopicDescriptor}.
 *
 * <p>Populated by {@link TopicSubscriptionManager} after it resolves the active
 * descriptors against the topic registry.  Read by {@link SatsKafkaListener} to
 * determine how each incoming message should be decoded.
 *
 * <p>Volatile write / concurrent reads — no locking needed.  The map is replaced
 * atomically on every {@link TopicSubscriptionManager#reload()}.
 */
@Component
@Slf4j
public class TopicRegistry {

    private volatile Map<String, SatsProperties.ConsumerConfig.TopicDescriptor> registry = Map.of();

    /**
     * Replaces the current registry with the supplied descriptors.
     * Called by {@link TopicSubscriptionManager} on every start / reload.
     */
    public void register(Collection<SatsProperties.ConsumerConfig.TopicDescriptor> descriptors) {
        this.registry = descriptors.stream()
                .collect(Collectors.toUnmodifiableMap(
                        SatsProperties.ConsumerConfig.TopicDescriptor::name,
                        d -> d
                ));
        log.info("TopicRegistry updated: {} topic(s) registered", registry.size());
    }

    /**
     * Looks up the descriptor for the given topic name.
     * Returns {@link Optional#empty()} for topics that were not in any configured group
     * (e.g. topics discovered via the registry but absent from the local config).
     */
    public Optional<SatsProperties.ConsumerConfig.TopicDescriptor> lookup(String topicName) {
        return Optional.ofNullable(registry.get(topicName));
    }
}
