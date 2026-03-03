# Schema-Aware Transformation Service — Project Scaffold

## Project Structure

```
schema-aware-transformation-service/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/com/sats/
│   │   │   ├── SatsApplication.java
│   │   │   ├── config/
│   │   │   │   ├── SatsProperties.java              # Root grouped config
│   │   │   │   ├── BatchProperties.java              # Per-dataset batch tuning
│   │   │   │   ├── ThreadingProperties.java          # Virtual/platform thread config
│   │   │   │   ├── RetryProperties.java              # Backoff retry config
│   │   │   │   ├── KafkaConsumerConfig.java          # Consumer factory & concurrency
│   │   │   │   └── ThreadingConfig.java              # ExecutorService bean wiring
│   │   │   ├── domain/
│   │   │   │   ├── enums/
│   │   │   │   │   ├── DataType.java                 # Target column types
│   │   │   │   │   ├── FlushProfile.java             # real-time / near-real-time / batch
│   │   │   │   │   ├── TargetType.java               # delta / parquet / json
│   │   │   │   │   └── DlqReason.java                # Failure categories
│   │   │   │   ├── model/
│   │   │   │   │   ├── FieldSpec.java                # Schema field descriptor
│   │   │   │   │   ├── SchemaDefinition.java         # Cached schema per dataset
│   │   │   │   │   ├── TransformedRecord.java        # Output envelope
│   │   │   │   │   ├── RescueEntry.java              # Single rescued field
│   │   │   │   │   └── BatchPayload.java             # Flushed batch for write pool
│   │   │   │   └── strategy/
│   │   │   │       ├── WriteStrategy.java            # Strategy interface
│   │   │   │       ├── DeltaWriteStrategy.java       # Delta writer
│   │   │   │       ├── ParquetWriteStrategy.java     # Parquet writer
│   │   │   │       └── JsonWriteStrategy.java        # JSON writer
│   │   │   ├── registry/
│   │   │   │   └── SchemaRegistryClient.java         # Fetch + TTL cache
│   │   │   ├── service/
│   │   │   │   ├── SchemaValidator.java              # Pattern-matching type checks
│   │   │   │   ├── TransformationEngine.java         # Extra-field & rescue loop
│   │   │   │   └── RecordStandardizer.java           # Envelope wrapper
│   │   │   ├── batch/
│   │   │   │   ├── BatchAccumulator.java             # Per-dataset buffer
│   │   │   │   └── PerDatasetBatchManager.java       # Lifecycle of accumulators
│   │   │   ├── consumer/
│   │   │   │   └── SatsKafkaListener.java            # Spring Kafka entry point
│   │   │   ├── writer/
│   │   │   │   ├── WriteProvider.java                # Strategy resolver + executor
│   │   │   │   └── SparkSessionManager.java          # Lazy init + token rotation
│   │   │   ├── dlq/
│   │   │   │   └── DlqProducer.java                  # Structured DLQ publishing
│   │   │   └── threading/
│   │   │       └── WritePoolExecutor.java            # Bounded platform-thread pool
│   │   └── resources/
│   │       ├── application.yml
│   │       └── logback-spring.xml
│   └── test/
│       └── java/com/sats/
│           └── SatsApplicationTests.java
```

## Design Patterns Applied

| Pattern | Where | Why |
|---------|-------|-----|
| **Strategy** | `WriteStrategy` + 3 implementations | Write logic differs per target type (Delta/Parquet/JSON); new targets added without touching existing code |
| **Factory Method** | `WriteProvider.resolve()` | Selects the correct `WriteStrategy` by `TargetType` enum |
| **Builder** | `TransformedRecord` (Lombok `@Builder`) | Complex object with many optional sidecar fields |
| **Template Method** | `BatchAccumulator.flush()` | Snapshot → reset → submit follows a fixed sequence; threshold evaluation is polymorphic |
| **Observer** | Micrometer metrics hooks | Counters/gauges fire on record processing, rescue, and DLQ events without coupling to business logic |

---

## File Contents

### `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.4.3</version>
        <relativePath/>
    </parent>

    <groupId>com.sats</groupId>
    <artifactId>schema-aware-transformation-service</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <name>SATS</name>
    <description>
        Kafka-to-Spark pipeline with schema validation, type rescue,
        and data standardization
    </description>

    <properties>
        <java.version>21</java.version>
        <spark.version>3.5.4</spark.version>
        <delta.version>3.3.0</delta.version>
        <lombok.version>1.18.36</lombok.version>
    </properties>

    <dependencies>
        <!-- Spring Boot Core -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-configuration-processor</artifactId>
            <optional>true</optional>
        </dependency>

        <!-- Kafka -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <!-- Spark (provided at runtime by cluster) -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.13</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-spark_2.13</artifactId>
            <version>${delta.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Serialization -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.datatype</groupId>
            <artifactId>jackson-datatype-jsr310</artifactId>
        </dependency>
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
            <version>4.29.3</version>
        </dependency>

        <!-- Observability -->
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>

        <!-- Lombok -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>${lombok.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Testing -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>21</source>
                    <target>21</target>
                    <annotationProcessorPaths>
                        <path>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                            <version>${lombok.version}</version>
                        </path>
                        <path>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-configuration-processor</artifactId>
                            <version>3.4.3</version>
                        </path>
                    </annotationProcessorPaths>
                </configuration>
            </plugin>
        </plugins>
    </build>

</project>
```

---

### `SatsApplication.java`

```java
package com.sats;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan("com.sats.config")
@EnableScheduling
public class SatsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SatsApplication.class, args);
    }
}
```

---

## Config Layer — `com.sats.config`

### `SatsProperties.java`

```java
package com.sats.config;

import com.sats.domain.enums.FlushProfile;
import com.sats.domain.enums.TargetType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

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
        CompactionConfig compaction,
        RetryProperties retry,
        DlqConfig dlq,
        SparkConfig spark
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

    public record CompactionConfig(
            boolean enabled,
            Duration interval,
            String targetFileSize
    ) {}

    public record DlqConfig(
            @NotBlank String topic
    ) {}

    public record SparkConfig(
            @Min(1) @Max(99) int tokenRefreshPercent
    ) {}
}
```

### `BatchProperties.java`

```java
package com.sats.config;

import com.sats.domain.enums.FlushProfile;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Per-dataset batching configuration.
 * Defaults align with the "near-real-time" profile from the design document.
 */
@ConfigurationProperties(prefix = "sats.batch")
public record BatchProperties(
        FlushProfile defaultProfile,
        String byteTarget,
        int maxRecords,
        Duration maxTimer,
        double highWaterMarkMultiplier
) {

    /** Sensible defaults when YAML entries are absent. */
    public BatchProperties {
        if (defaultProfile == null) defaultProfile = FlushProfile.NEAR_REAL_TIME;
        if (byteTarget == null) byteTarget = "128MB";
        if (maxRecords <= 0) maxRecords = 50_000;
        if (maxTimer == null) maxTimer = Duration.ofSeconds(60);
        if (highWaterMarkMultiplier <= 0) highWaterMarkMultiplier = 2.0;
    }
}
```

### `ThreadingProperties.java`

```java
package com.sats.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Threading and concurrency configuration.
 * Governs the platform-thread write pool and virtual-thread carrier pool.
 */
@ConfigurationProperties(prefix = "sats.threading")
public record ThreadingProperties(
        int writePoolSize,
        int writeQueueCapacity,
        int carrierPoolSize,
        Duration pinAlertThreshold
) {

    public ThreadingProperties {
        if (writePoolSize <= 0) writePoolSize = 8;
        if (writeQueueCapacity <= 0) writeQueueCapacity = writePoolSize * 2;
        if (carrierPoolSize <= 0) carrierPoolSize = Runtime.getRuntime().availableProcessors();
        if (pinAlertThreshold == null) pinAlertThreshold = Duration.ofMillis(100);
    }
}
```

### `RetryProperties.java`

```java
package com.sats.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Exponential-backoff retry configuration for Spark write failures.
 */
@ConfigurationProperties(prefix = "sats.retry")
public record RetryProperties(
        int maxAttempts,
        Duration initialBackoff,
        double backoffMultiplier
) {

    public RetryProperties {
        if (maxAttempts <= 0) maxAttempts = 5;
        if (initialBackoff == null) initialBackoff = Duration.ofSeconds(1);
        if (backoffMultiplier <= 0) backoffMultiplier = 2.0;
    }
}
```

### `KafkaConsumerConfig.java`

```java
package com.sats.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Map;

/**
 * Configures the Kafka consumer factory with byte-array value deserialization.
 * Deserialization into domain objects happens in the transformation layer so
 * that parse failures can be routed to the DLQ instead of crashing the listener.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setBatchListener(true);
        log.info("Kafka listener container factory initialised with MANUAL_IMMEDIATE ack and batch mode");
        return factory;
    }
}
```

### `ThreadingConfig.java`

```java
package com.sats.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Wires the two executor tiers described in Section 9 of the design document:
 * <ul>
 *   <li><b>virtualExecutor</b> — unbounded virtual-thread pool for polling,
 *       deserialization, and transformation (I/O-bound, no {@code synchronized}).</li>
 *   <li><b>writePoolExecutor</b> — bounded platform-thread pool for Spark writes
 *       (avoids virtual-thread carrier pinning inside Spark's synchronized blocks).</li>
 * </ul>
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class ThreadingConfig {

    private final ThreadingProperties threadingProps;

    @Bean
    public ExecutorService virtualExecutor() {
        log.info("Creating virtual-thread executor (carrier pool size: {})", threadingProps.carrierPoolSize());
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    public ThreadPoolExecutor writePoolExecutor() {
        var executor = new ThreadPoolExecutor(
                threadingProps.writePoolSize(),
                threadingProps.writePoolSize(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(threadingProps.writeQueueCapacity()),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        log.info(
                "Write pool initialised: poolSize={}, queueCapacity={}, rejectionPolicy=CallerRunsPolicy",
                threadingProps.writePoolSize(),
                threadingProps.writeQueueCapacity()
        );
        return executor;
    }
}
```

---

## Domain Layer — `com.sats.domain`

### `enums/DataType.java`

```java
package com.sats.domain.enums;

/**
 * Supported target column data types, aligned with Spark SQL types.
 */
public enum DataType {
    STRING,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    TIMESTAMP,
    DATE,
    BINARY,
    STRUCT,
    ARRAY,
    MAP
}
```

### `enums/FlushProfile.java`

```java
package com.sats.domain.enums;

import java.time.Duration;

/**
 * Pre-defined flush profiles from Section 8.5 of the design document.
 * Each profile balances file size against ingestion latency.
 */
public enum FlushProfile {

    REAL_TIME("32MB", 10_000, Duration.ofSeconds(10)),
    NEAR_REAL_TIME("128MB", 50_000, Duration.ofSeconds(60)),
    BATCH_OPTIMIZED("256MB", 100_000, Duration.ofSeconds(300));

    private final String byteTarget;
    private final int maxRecords;
    private final Duration maxTimer;

    FlushProfile(String byteTarget, int maxRecords, Duration maxTimer) {
        this.byteTarget = byteTarget;
        this.maxRecords = maxRecords;
        this.maxTimer = maxTimer;
    }

    public String byteTarget() { return byteTarget; }
    public int maxRecords() { return maxRecords; }
    public Duration maxTimer() { return maxTimer; }
}
```

### `enums/TargetType.java`

```java
package com.sats.domain.enums;

/**
 * Supported Spark write destinations.
 */
public enum TargetType {
    DELTA,
    PARQUET,
    JSON
}
```

### `enums/DlqReason.java`

```java
package com.sats.domain.enums;

/**
 * Failure categories used as the {@code reason} header on DLQ messages.
 */
public enum DlqReason {
    PARSE_FAILURE,
    SCHEMA_MISMATCH,
    EXCESSIVE_RESCUE,
    WRITE_FAILURE
}
```

### `model/FieldSpec.java`

```java
package com.sats.domain.model;

import com.sats.domain.enums.DataType;

/**
 * Describes a single field in the target schema.
 * Used as the value type in the flattened schema lookup map.
 */
public record FieldSpec(
        String fieldName,
        DataType targetType,
        boolean nullable,
        String[] nestedPath
) {}
```

### `model/SchemaDefinition.java`

```java
package com.sats.domain.model;

import java.time.Instant;
import java.util.Map;

/**
 * Cached schema for a single dataset.
 * The {@code fields} map is the primary lookup structure for Phase 2 transformation.
 */
public record SchemaDefinition(
        String datasetId,
        Map<String, FieldSpec> fields,
        Instant fetchedAt
) {}
```

### `model/TransformedRecord.java`

```java
package com.sats.domain.model;

import lombok.Builder;

import java.time.Instant;
import java.util.Map;

/**
 * The output envelope emitted by the TransformationEngine (Section 5).
 * Carries the validated payload alongside rescue, extra, and Kafka metadata.
 */
@Builder
public record TransformedRecord(
        Map<String, Object> schemaAlignedFields,
        Map<String, Object> rescueData,
        Map<String, Object> additionalColumns,
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset,
        Instant ingestionTimestamp
) {}
```

### `model/RescueEntry.java`

```java
package com.sats.domain.model;

import com.sats.domain.enums.DataType;

/**
 * Captures a single field that failed type validation.
 */
public record RescueEntry(
        String fieldName,
        Object originalValue,
        DataType expectedType,
        String failureReason
) {}
```

### `model/BatchPayload.java`

```java
package com.sats.domain.model;

import java.util.List;

/**
 * Immutable snapshot of a flushed batch, handed off from the
 * {@code BatchAccumulator} to the platform-thread write pool.
 */
public record BatchPayload(
        String datasetId,
        List<TransformedRecord> records,
        long batchSizeBytes,
        String batchId
) {}
```

---

## Domain Strategy Layer — `com.sats.domain.strategy`

### `WriteStrategy.java`

```java
package com.sats.domain.strategy;

import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;

/**
 * Strategy interface for writing a batch to a Spark-managed destination.
 * Implementations are selected by the {@link com.sats.writer.WriteProvider}
 * based on the configured {@link TargetType}.
 */
public interface WriteStrategy {

    /** Returns the target type this strategy handles. */
    TargetType supports();

    /** Executes the write for the given batch payload. */
    void write(BatchPayload payload);
}
```

### `DeltaWriteStrategy.java`

```java
package com.sats.domain.strategy;

import com.sats.config.SatsProperties;
import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class DeltaWriteStrategy implements WriteStrategy {

    private final SatsProperties satsProperties;

    @Override
    public TargetType supports() {
        return TargetType.DELTA;
    }

    @Override
    public void write(BatchPayload payload) {
        log.info(
                "Writing batch {} ({} records, {} bytes) to Delta target for dataset {}",
                payload.batchId(), payload.records().size(),
                payload.batchSizeBytes(), payload.datasetId()
        );
        // TODO: Convert payload.records() to Dataset<Row> via SparkSessionManager,
        //       then write with format("delta").mode("append").option("mergeSchema", mergeSchema)
    }
}
```

### `ParquetWriteStrategy.java`

```java
package com.sats.domain.strategy;

import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ParquetWriteStrategy implements WriteStrategy {

    @Override
    public TargetType supports() {
        return TargetType.PARQUET;
    }

    @Override
    public void write(BatchPayload payload) {
        log.info(
                "Writing batch {} ({} records) to Parquet target for dataset {}",
                payload.batchId(), payload.records().size(), payload.datasetId()
        );
        // TODO: Convert to Dataset<Row>, write with format("parquet").mode("append")
    }
}
```

### `JsonWriteStrategy.java`

```java
package com.sats.domain.strategy;

import com.sats.domain.enums.TargetType;
import com.sats.domain.model.BatchPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JsonWriteStrategy implements WriteStrategy {

    @Override
    public TargetType supports() {
        return TargetType.JSON;
    }

    @Override
    public void write(BatchPayload payload) {
        log.info(
                "Writing batch {} ({} records) to JSON target for dataset {}",
                payload.batchId(), payload.records().size(), payload.datasetId()
        );
        // TODO: Convert to Dataset<Row>, write with format("json").mode("append")
    }
}
```

---

## Registry Layer — `com.sats.registry`

### `SchemaRegistryClient.java`

```java
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
```

---

## Service Layer — `com.sats.service`

### `SchemaValidator.java`

```java
package com.sats.service;

import com.sats.domain.enums.DataType;
import com.sats.domain.model.FieldSpec;
import com.sats.domain.model.RescueEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

/**
 * Performs safe-cast type validation using Java 21 pattern matching (Section 4.3).
 * Returns the coerced value on success or a {@link RescueEntry} on failure.
 */
@Service
@Slf4j
public class SchemaValidator {

    /**
     * Attempts to cast {@code rawValue} to the type declared in {@code spec}.
     *
     * @return An {@link Optional} containing the cast value, or empty if rescue is required.
     */
    public Optional<Object> safeCast(Object rawValue, FieldSpec spec) {
        if (rawValue == null) {
            return spec.nullable() ? Optional.empty() : Optional.empty();
        }
        return switch (spec.targetType()) {
            case STRING  -> Optional.of(rawValue.toString());
            case DOUBLE  -> toDouble(rawValue);
            case LONG    -> toLong(rawValue);
            case INTEGER -> toInteger(rawValue);
            case BOOLEAN -> toBoolean(rawValue);
            case TIMESTAMP -> toTimestamp(rawValue);
            default -> {
                log.warn("No safe-cast path for target type {} on field {}",
                        spec.targetType(), spec.fieldName());
                yield Optional.empty();
            }
        };
    }

    public RescueEntry rescue(String fieldName, Object rawValue, DataType expected, String reason) {
        return new RescueEntry(fieldName, rawValue, expected, reason);
    }

    // ── Private cast helpers ──────────────────────────────────────────

    private Optional<Object> toDouble(Object raw) {
        return switch (raw) {
            case Double d   -> Optional.of(d);
            case Number n   -> Optional.of(n.doubleValue());
            case String s   -> parseDouble(s);
            default         -> Optional.empty();
        };
    }

    private Optional<Object> toLong(Object raw) {
        return switch (raw) {
            case Long l     -> Optional.of(l);
            case Integer i  -> Optional.of((long) i);
            case String s   -> parseLong(s);
            default         -> Optional.empty();
        };
    }

    private Optional<Object> toInteger(Object raw) {
        return switch (raw) {
            case Integer i  -> Optional.of(i);
            case Long l when l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE
                            -> Optional.of(l.intValue());
            case String s   -> parseInt(s);
            default         -> Optional.empty();
        };
    }

    private Optional<Object> toBoolean(Object raw) {
        return switch (raw) {
            case Boolean b  -> Optional.of(b);
            case String s when "true".equalsIgnoreCase(s)  -> Optional.of(true);
            case String s when "false".equalsIgnoreCase(s) -> Optional.of(false);
            default         -> Optional.empty();
        };
    }

    private Optional<Object> toTimestamp(Object raw) {
        return switch (raw) {
            case Timestamp t -> Optional.of(t);
            case Long epoch  -> Optional.of(Timestamp.from(Instant.ofEpochMilli(epoch)));
            case String s    -> parseTimestamp(s);
            default          -> Optional.empty();
        };
    }

    private Optional<Object> parseDouble(String s) {
        try { return Optional.of(Double.parseDouble(s)); }
        catch (NumberFormatException e) { return Optional.empty(); }
    }

    private Optional<Object> parseLong(String s) {
        try { return Optional.of(Long.parseLong(s)); }
        catch (NumberFormatException e) { return Optional.empty(); }
    }

    private Optional<Object> parseInt(String s) {
        try { return Optional.of(Integer.parseInt(s)); }
        catch (NumberFormatException e) { return Optional.empty(); }
    }

    private Optional<Object> parseTimestamp(String s) {
        try { return Optional.of(Timestamp.from(Instant.parse(s))); }
        catch (Exception e) { return Optional.empty(); }
    }
}
```

### `TransformationEngine.java`

```java
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
```

### `RecordStandardizer.java`

```java
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
```

---

## Batch Layer — `com.sats.batch`

### `BatchAccumulator.java`

```java
package com.sats.batch;

import com.sats.domain.model.BatchPayload;
import com.sats.domain.model.TransformedRecord;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-dataset buffer with three-trigger flush policy (Section 8.2).
 * Uses {@link ReentrantLock} instead of {@code synchronized} to avoid
 * virtual-thread carrier pinning.
 */
@Slf4j
public class BatchAccumulator {

    private final String datasetId;
    private final int maxRecords;
    private final long maxBytes;
    private final Duration maxTimer;

    private final ReentrantLock lock = new ReentrantLock();
    private List<TransformedRecord> buffer;
    private long currentBytes;
    private Instant windowStart;

    public BatchAccumulator(String datasetId, int maxRecords, long maxBytes, Duration maxTimer) {
        this.datasetId = datasetId;
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
        this.maxTimer = maxTimer;
        this.buffer = new ArrayList<>();
        this.currentBytes = 0;
        this.windowStart = Instant.now();
    }

    /**
     * Adds a record and returns a flush payload if any threshold is breached,
     * or {@code null} if the batch is still accumulating.
     */
    public BatchPayload add(TransformedRecord record, long estimatedBytes) {
        lock.lock();
        try {
            buffer.add(record);
            currentBytes += estimatedBytes;

            if (shouldFlush()) {
                return snapshot();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Timer-driven flush: returns a payload if any records are buffered, else null.
     */
    public BatchPayload timerFlush() {
        lock.lock();
        try {
            if (buffer.isEmpty()) return null;
            if (Instant.now().isAfter(windowStart.plus(maxTimer))) {
                return snapshot();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /** Zero-copy buffer swap: captures the current buffer and resets state. */
    private BatchPayload snapshot() {
        var flushed = buffer;
        var flushedBytes = currentBytes;

        // Reset — new list avoids copying
        buffer = new ArrayList<>();
        currentBytes = 0;
        windowStart = Instant.now();

        log.debug("Flushing batch for dataset {}: {} records, {} bytes",
                datasetId, flushed.size(), flushedBytes);

        return new BatchPayload(
                datasetId,
                List.copyOf(flushed),
                flushedBytes,
                UUID.randomUUID().toString()
        );
    }

    private boolean shouldFlush() {
        return buffer.size() >= maxRecords
                || currentBytes >= maxBytes
                || Instant.now().isAfter(windowStart.plus(maxTimer));
    }

    public int bufferSize() { return buffer.size(); }
    public long bufferBytes() { return currentBytes; }
}
```

### `PerDatasetBatchManager.java`

```java
package com.sats.batch;

import com.sats.config.BatchProperties;
import com.sats.domain.model.BatchPayload;
import com.sats.domain.model.TransformedRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Manages per-dataset {@link BatchAccumulator} instances (Section 8.2).
 * A scheduled timer periodically checks all accumulators for time-based flushes.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PerDatasetBatchManager {

    private final BatchProperties batchProperties;
    private final ConcurrentMap<String, BatchAccumulator> accumulators = new ConcurrentHashMap<>();

    /** Callback invoked when a batch is ready to be submitted to the write pool. */
    private Consumer<BatchPayload> flushCallback = payload ->
            log.warn("No flush callback registered — batch {} for dataset {} dropped",
                    payload.batchId(), payload.datasetId());

    public void registerFlushCallback(Consumer<BatchPayload> callback) {
        this.flushCallback = callback;
    }

    public void accumulate(String datasetId, TransformedRecord record, long estimatedBytes) {
        var accumulator = accumulators.computeIfAbsent(datasetId, this::createAccumulator);
        var payload = accumulator.add(record, estimatedBytes);
        if (payload != null) {
            flushCallback.accept(payload);
        }
    }

    @Scheduled(fixedDelayString = "${sats.batch.timer-check-interval-ms:5000}")
    public void timerFlushAll() {
        accumulators.forEach((datasetId, accumulator) -> {
            var payload = accumulator.timerFlush();
            if (payload != null) {
                flushCallback.accept(payload);
            }
        });
    }

    private BatchAccumulator createAccumulator(String datasetId) {
        log.info("Creating batch accumulator for dataset {}", datasetId);
        return new BatchAccumulator(
                datasetId,
                batchProperties.maxRecords(),
                parseBytes(batchProperties.byteTarget()),
                batchProperties.maxTimer()
        );
    }

    private long parseBytes(String size) {
        String upper = size.toUpperCase().trim();
        if (upper.endsWith("GB")) return Long.parseLong(upper.replace("GB", "").trim()) * 1024 * 1024 * 1024;
        if (upper.endsWith("MB")) return Long.parseLong(upper.replace("MB", "").trim()) * 1024 * 1024;
        if (upper.endsWith("KB")) return Long.parseLong(upper.replace("KB", "").trim()) * 1024;
        return Long.parseLong(upper);
    }
}
```

---

## Consumer Layer — `com.sats.consumer`

### `SatsKafkaListener.java`

```java
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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Entry point for Kafka message consumption. Receives byte-array batches,
 * deserialises each record, runs it through the transformation engine,
 * and pushes results into the per-dataset batch manager.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SatsKafkaListener {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;
    private final SchemaRegistryClient schemaRegistryClient;
    private final TransformationEngine transformationEngine;
    private final PerDatasetBatchManager batchManager;
    private final DlqProducer dlqProducer;

    @KafkaListener(
            topics = "${sats.consumer.topics}",
            groupId = "${sats.consumer.group-id:sats-consumer-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onBatch(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) {
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
```

---

## Writer Layer — `com.sats.writer`

### `WriteProvider.java`

```java
package com.sats.writer;

import com.sats.config.SatsProperties;
import com.sats.domain.model.BatchPayload;
import com.sats.domain.strategy.WriteStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Factory/resolver for {@link WriteStrategy} implementations.
 * Selects the strategy matching the configured target type and delegates the write.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class WriteProvider {

    private final SatsProperties satsProperties;
    private final List<WriteStrategy> strategies;

    public void write(BatchPayload payload) {
        var targetType = satsProperties.writer().targetType();

        strategies.stream()
                .filter(s -> s.supports() == targetType)
                .findFirst()
                .ifPresentOrElse(
                        strategy -> strategy.write(payload),
                        () -> log.error("No WriteStrategy registered for target type: {}", targetType)
                );
    }
}
```

### `SparkSessionManager.java`

```java
package com.sats.writer;

import com.sats.config.SatsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Manages Spark session lifecycle with lazy initialisation and
 * fail-then-refresh recovery (Section 6.1).
 * <p>
 * Token rotation is handled by scheduling a refresh at 80% of the token TTL.
 * On unexpected 401, an immediate synchronous refresh is triggered.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SparkSessionManager {

    private final SatsProperties satsProperties;

    // TODO: LazyInitializer<SparkSession> with fail-then-refresh logic
    //       TokenProvider integration for PAT / OAuth lifecycle

    public void ensureSession() {
        log.debug("Ensuring Spark session is active (token refresh at {}% TTL)",
                satsProperties.spark().tokenRefreshPercent());
    }
}
```

---

## DLQ Layer — `com.sats.dlq`

### `DlqProducer.java`

```java
package com.sats.dlq;

import com.sats.config.SatsProperties;
import com.sats.domain.enums.DlqReason;
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

    private void addHeader(ProducerRecord<String, byte[]> record, String key, String value) {
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    private String truncate(String value, int maxLength) {
        if (value == null) return "";
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
```

---

## Threading Layer — `com.sats.threading`

### `WritePoolExecutor.java`

```java
package com.sats.threading;

import com.sats.domain.model.BatchPayload;
import com.sats.writer.WriteProvider;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Submits flushed batches to the bounded platform-thread write pool (Section 9.5).
 * Exposes pool metrics via Micrometer.
 */
@Component
@Slf4j
public class WritePoolExecutor {

    private final ThreadPoolExecutor writePool;
    private final WriteProvider writeProvider;

    public WritePoolExecutor(
            ThreadPoolExecutor writePoolExecutor,
            WriteProvider writeProvider,
            MeterRegistry meterRegistry
    ) {
        this.writePool = writePoolExecutor;
        this.writeProvider = writeProvider;

        // Observability (Section 7.3)
        meterRegistry.gauge("sats.write.pool.active", writePool, ThreadPoolExecutor::getActiveCount);
        meterRegistry.gauge("sats.write.pool.queue.depth", writePool, e -> e.getQueue().size());
    }

    /**
     * Submits a batch for async write on the platform-thread pool.
     * If the pool and queue are both saturated, {@code CallerRunsPolicy} executes
     * the write on the calling (virtual) thread — providing natural backpressure.
     */
    public void submit(BatchPayload payload) {
        writePool.execute(() -> {
            try {
                writeProvider.write(payload);
            } catch (Exception e) {
                log.error("Write failed for batch {} (dataset {}): {}",
                        payload.batchId(), payload.datasetId(), e.getMessage(), e);
                // TODO: Retry with backoff or route batch metadata to DLQ
            }
        });
    }
}
```

---

## Resources

### `application.yml`

```yaml
# ═══════════════════════════════════════════════════════════
# Schema-Aware Transformation Service — Default Configuration
# ═══════════════════════════════════════════════════════════

sats:
  schema:
    cache-ttl: 5m
    registry-url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
    rescue-threshold-percent: 50

  batch:
    default-profile: near-real-time
    byte-target: 128MB
    max-records: 50000
    max-timer: 60s
    high-water-mark-multiplier: 2.0
    timer-check-interval-ms: 5000

  threading:
    write-pool-size: 8
    write-queue-capacity: 16
    carrier-pool-size: 0          # 0 = auto (availableProcessors)
    pin-alert-threshold: 100ms

  writer:
    target-type: delta
    merge-schema: true

  compaction:
    enabled: true
    interval: 4h
    target-file-size: 1GB

  retry:
    max-attempts: 5
    initial-backoff: 1s
    backoff-multiplier: 2.0

  dlq:
    topic: ${DLQ_TOPIC:sats-dlq}

  spark:
    token-refresh-percent: 80

  consumer:
    topics: ${KAFKA_TOPICS:}
    group-id: sats-consumer-group
    group-strategy: format

# ─── Spring Kafka defaults ────────────────────────────────
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 500

# ─── Actuator & Metrics ──────────────────────────────────
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  metrics:
    tags:
      application: sats
```

### `logback-spring.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <include resource="org/springframework/boot/logging/logback/defaults.xml"/>

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>
                %d{ISO8601} [%thread] %-5level %logger{36}
                [topic=%X{topic_name:-} dataset=%X{dataset_id:-} batch=%X{batch_id:-} corr=%X{correlation_id:-}]
                - %msg%n
            </pattern>
        </encoder>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>

    <logger name="com.sats" level="DEBUG"/>
    <logger name="org.apache.kafka" level="WARN"/>
    <logger name="org.apache.spark" level="WARN"/>
</configuration>
```

---

### `SatsApplicationTests.java`

```java
package com.sats;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {
        "sats.schema.registry-url=http://localhost:8081",
        "sats.dlq.topic=test-dlq",
        "sats.consumer.topics=test-topic",
        "spring.kafka.bootstrap-servers=localhost:9092"
})
class SatsApplicationTests {

    @Test
    void contextLoads() {
        // Verifies Spring context wires all beans without circular dependencies
    }
}
```

---

## Getting Started

```bash
# Generate from this scaffold
mkdir -p schema-aware-transformation-service
cd schema-aware-transformation-service

# Copy the files into the structure shown above, then:
mvn clean compile          # Verify compilation with Java 21
mvn spring-boot:run        # Requires Kafka + Schema Registry running locally

# Or run with Docker Compose (add your own docker-compose.yml with Kafka/ZK)
```

## Next Steps

| Priority | Task | Section |
|----------|------|---------|
| 1 | Implement `SchemaRegistryClient.refreshSchema()` with real HTTP calls | §3 |
| 2 | Wire `WritePoolExecutor` as the flush callback in `PerDatasetBatchManager` | §8.2 |
| 3 | Implement `DeltaWriteStrategy.write()` with Spark `Dataset<Row>` conversion | §6.3 |
| 4 | Add `CompactionJob` scheduled bean for Delta OPTIMIZE | §8.3 |
| 5 | Integration tests with Testcontainers (Kafka) + embedded Spark | §11.4 |
| 6 | JFR pinning monitor and alerting hook | §9.6 |