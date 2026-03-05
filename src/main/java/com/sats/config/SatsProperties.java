package com.sats.config;

import com.sats.domain.enums.MessageType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

/**
 * Root configuration properties for the Schema-Aware Transformation Service.
 * Maps to the {@code sats.*} namespace in application.yml.
 */
@Validated
@ConfigurationProperties(prefix = "sats")
public record SatsProperties(
        SchemaConfig schema,
        BatchProperties batch,
        ThreadingProperties threading,
        WriterConfig writer,
        RetryProperties retry,
        DlqConfig dlq,
        SparkConfig spark,
        ConsumerConfig consumer
) {

    public record SchemaConfig(
            Duration cacheTtl,
            @NotBlank String registryUrl,
            @Min(1) @Max(100) int rescueThresholdPercent
    ) {}

    public record WriterConfig(
            boolean mergeSchema
    ) {}

    public record DlqConfig(
            @NotBlank String topic
    ) {}

    public record SparkConfig(
            @Min(1) @Max(99) int tokenRefreshPercent,
            Duration tokenTtl,
            String master,
            String appName,
            String outputBasePath
    ) {
        public SparkConfig {
            if (tokenTtl == null)        tokenTtl       = Duration.ofHours(1);
            if (master == null)          master         = "local[*]";
            if (appName == null)         appName        = "SATS";
            if (outputBasePath == null)  outputBasePath = "/tmp/sats";
        }
    }

    /**
     * Consumer topology configuration.
     * {@code topicGroups} defines how topics fetched from the REST registry are
     * partitioned into independent listener containers.  Topics that appear in the
     * same group share one poll loop; topics in different groups run on separate
     * threads and cannot starve each other.
     */
    public record ConsumerConfig(
            @NotBlank String topicRegistryUrl,
            String groupId,
            List<TopicGroup> topicGroups
    ) {

        /**
         * Describes a single Kafka topic — its name, message type, and (for
         * {@code PLAIN_TEXT}) the dataset ID that provides schema + write-format.
         *
         * <ul>
         *   <li>{@code DATA_RECORD} / {@code DATA_LOAD} — dataset ID is embedded
         *       inside the Protobuf envelope; {@code datasetId} is ignored.</li>
         *   <li>{@code PLAIN_TEXT} — {@code datasetId} must be set; it is used to
         *       look up the target schema and write format.</li>
         * </ul>
         */
        public record TopicDescriptor(
                @NotBlank String name,
                @NotNull  MessageType type,
                String datasetId          // required for PLAIN_TEXT; null otherwise
        ) {}

        /**
         * A named set of topics that share one {@code ConcurrentMessageListenerContainer}.
         *
         * <ul>
         *   <li>{@code topics} — descriptors whose {@code name} is cross-referenced
         *       against the REST topic registry; unmatched descriptors are skipped.</li>
         *   <li>{@code concurrency} — number of {@code KafkaConsumer} threads.
         *       Effective parallelism = min(concurrency, partition count).</li>
         * </ul>
         */
        public record TopicGroup(
                @NotEmpty List<TopicDescriptor> topics,
                int concurrency
        ) {
            public TopicGroup {
                if (concurrency <= 0) concurrency = 1;
            }
        }
    }
}
