package com.sats.config;

import com.sats.domain.enums.TargetType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
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
            TargetType targetType,
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
         * A named set of topics that share one {@code ConcurrentMessageListenerContainer}.
         *
         * <ul>
         *   <li>{@code topics} — subset of topics (from the REST registry) to assign to
         *       this group.  Topics not listed in any group fall into an implicit default
         *       container with {@code concurrency = 1}.</li>
         *   <li>{@code concurrency} — number of {@code KafkaConsumer} threads.
         *       Effective parallelism = min(concurrency, partition count).</li>
         * </ul>
         */
        public record TopicGroup(
                @NotEmpty List<String> topics,
                int concurrency
        ) {
            public TopicGroup {
                if (concurrency <= 0) concurrency = 1;
            }
        }
    }
}
