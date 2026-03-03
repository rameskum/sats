package com.sats.consumer;

import com.sats.config.SatsProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of one or more Kafka listener containers.
 *
 * <h3>Topology</h3>
 * Topics are fetched from the REST registry via {@link TopicProvider}, then
 * distributed across containers according to {@code sats.consumer.topic-groups}:
 *
 * <pre>
 *   topic-groups:
 *     - topics: [orders, payments]   # shared poll loop — similar volume
 *       concurrency: 2               # 2 KafkaConsumers → 2 parallel poll threads
 *     - topics: [clickstream]        # isolated — high volume
 *       concurrency: 4
 * </pre>
 *
 * Topics returned by the registry but not listed in any group are collected into
 * an implicit <em>default</em> container with {@code concurrency = 1}.
 *
 * <h3>Concurrency vs partitions</h3>
 * Each unit of concurrency is a separate {@code KafkaConsumer} in the same
 * consumer group.  Kafka assigns partitions across them, so effective parallelism
 * is {@code min(concurrency, partitionCount)}.  Extra consumers beyond the
 * partition count sit idle and waste a thread — keep concurrency ≤ partitions.
 *
 * <h3>Reload</h3>
 * {@link #reload()} stops all containers, re-fetches topics from the REST registry,
 * and rebuilds containers from the current group config — no JVM restart needed.
 */
@Component
@Slf4j
public class TopicSubscriptionManager implements SmartLifecycle {

    private final TopicProvider topicProvider;
    private final SatsKafkaListener kafkaListener;
    private final ConsumerFactory<String, byte[]> consumerFactory;
    private final SatsProperties satsProperties;

    // key = group label for logging; value = running container
    private final Map<String, ConcurrentMessageListenerContainer<String, byte[]>> containers
            = new ConcurrentHashMap<>();

    private volatile boolean running = false;

    public TopicSubscriptionManager(
            TopicProvider topicProvider,
            SatsKafkaListener kafkaListener,
            ConsumerFactory<String, byte[]> consumerFactory,
            SatsProperties satsProperties
    ) {
        this.topicProvider = topicProvider;
        this.kafkaListener = kafkaListener;
        this.consumerFactory = consumerFactory;
        this.satsProperties = satsProperties;
    }

    // ── SmartLifecycle ────────────────────────────────────────────────

    @Override
    public void start() {
        var consumerConfig = satsProperties.consumer();
        String[] allTopics = topicProvider.getTopics();
        Set<String> registryTopics = new HashSet<>(Arrays.asList(allTopics));
        Set<String> assignedTopics = new HashSet<>();

        var groups = consumerConfig.topicGroups();

        if (groups != null && !groups.isEmpty()) {
            for (var group : groups) {
                // Only subscribe to topics that the registry actually returned
                List<String> matched = group.topics().stream()
                        .filter(registryTopics::contains)
                        .collect(Collectors.toList());

                if (matched.isEmpty()) {
                    log.warn("Topic group {} has no matches in registry — skipping", group.topics());
                    continue;
                }

                String label = String.join("+", matched);
                startContainer(label, matched.toArray(String[]::new), group.concurrency());
                assignedTopics.addAll(matched);
            }
        }

        // Topics from registry not claimed by any group → implicit default container
        Set<String> ungrouped = new HashSet<>(registryTopics);
        ungrouped.removeAll(assignedTopics);
        if (!ungrouped.isEmpty()) {
            log.info("Ungrouped topics → default container (concurrency=1): {}", ungrouped);
            startContainer("__default__", ungrouped.toArray(String[]::new), 1);
        }

        running = true;
    }

    @Override
    public void stop() {
        containers.values().forEach(c -> {
            if (c.isRunning()) c.stop();
        });
        containers.clear();
        running = false;
        log.info("All Kafka listener container(s) stopped");
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** High phase = starts late, stops early — after all business beans are ready. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    // ── Public control API ────────────────────────────────────────────

    /**
     * Re-fetches topics from the REST registry and rebuilds all containers.
     * Group config is re-read from the current {@link SatsProperties}, so a
     * config-server refresh followed by {@code reload()} picks up both new
     * topics and updated group assignments.
     */
    public synchronized void reload() {
        log.info("Reloading all Kafka listener container(s)...");
        stop();
        start();
    }

    // ── Internal ──────────────────────────────────────────────────────

    private void startContainer(String label, String[] topics, int concurrency) {
        var props = new ContainerProperties(topics);
        props.setGroupId(satsProperties.consumer().groupId());
        props.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        props.setMessageListener(kafkaListener);

        var container = new ConcurrentMessageListenerContainer<>(consumerFactory, props);
        container.setConcurrency(concurrency);
        container.start();

        containers.put(label, container);
        log.info("Container [{}] started — topics={}, concurrency={}",
                label, Arrays.toString(topics), concurrency);
    }
}
