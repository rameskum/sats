package com.sats.replay;

import com.sats.config.SatsProperties;
import com.sats.consumer.RecordProcessor;
import com.sats.domain.enums.MessageType;
import com.sats.store.RawMessageStore;
import com.sats.writer.SparkSessionManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Replays raw Kafka messages from the {@code __raw_messages__} Delta table by
 * feeding each stored record back through {@link RecordProcessor}.
 *
 * <h3>Why Delta table, not Kafka seek?</h3>
 * The raw table stores messages indefinitely, independent of Kafka's retention
 * policy.  This means failed transformations can be replayed weeks later after
 * a schema fix, even if the original Kafka data has been compacted or expired.
 *
 * <h3>Threading</h3>
 * Spark operations are submitted to the platform-thread write pool to avoid
 * virtual-thread carrier pinning inside Spark's {@code synchronized} blocks.
 * The REST caller awaits the {@link CompletableFuture} while parked (not pinned)
 * on its virtual thread.
 *
 * <h3>Safety</h3>
 * {@link ReplayRequest#limit()} caps the number of rows collected into memory.
 * Use smaller ranges (narrow offset window) for large topics.
 */
@Service
@Slf4j
public class ReplayService {

    private final SparkSessionManager sparkSessionManager;
    private final SatsProperties satsProperties;
    private final RecordProcessor recordProcessor;
    private final ThreadPoolExecutor writePool;

    public ReplayService(
            SparkSessionManager sparkSessionManager,
            SatsProperties satsProperties,
            RecordProcessor recordProcessor,
            ThreadPoolExecutor writePoolExecutor
    ) {
        this.sparkSessionManager = sparkSessionManager;
        this.satsProperties = satsProperties;
        this.recordProcessor = recordProcessor;
        this.writePool = writePoolExecutor;
    }

    /**
     * Asynchronously replays messages matching the request.
     * The future completes when all matched records have been processed.
     * Runs on the platform-thread write pool — safe for Spark calls.
     */
    public CompletableFuture<ReplayResult> replay(ReplayRequest request) {
        log.info("Replay requested: topic={}, partition={}, offsets=[{},{}], type={}, limit={}",
                request.topic(), request.partition(),
                request.startOffset(), request.endOffset(),
                request.messageType(), request.limit());
        return CompletableFuture.supplyAsync(() -> executeReplay(request), writePool);
    }

    // ── Core replay logic (runs on platform thread) ───────────────────────────

    private ReplayResult executeReplay(ReplayRequest request) {
        String tablePath = satsProperties.spark().outputBasePath() + "/" + RawMessageStore.RAW_TABLE;
        var spark = sparkSessionManager.getSession();

        // ── Build the query ───────────────────────────────────────────────────
        var df = spark.read().format("delta").load(tablePath);

        df = df.filter(functions.col("topic").equalTo(request.topic()));

        if (request.partition() != null) {
            df = df.filter(functions.col("partition").equalTo((long) request.partition()));
        }
        df = df.filter(functions.col("offset").geq(request.startOffset()));
        if (request.endOffset() != null) {
            df = df.filter(functions.col("offset").leq(request.endOffset()));
        }
        if (request.messageType() != null) {
            df = df.filter(functions.col("messageType").equalTo(request.messageType().name()));
        }

        df = df.orderBy(functions.col("partition"), functions.col("offset"))
               .limit(request.limit());

        var rows = df.collectAsList();
        int totalFound = rows.size();
        log.info("Replay: {} row(s) found for topic={}", totalFound, request.topic());

        // ── Process each row ─────────────────────────────────────────────────
        int succeeded = 0;
        int failed = 0;
        List<String> errors = new ArrayList<>();

        for (var row : rows) {
            String topic        = row.getString(row.fieldIndex("topic"));
            int    partition    = (int) row.getLong(row.fieldIndex("partition"));
            long   offset       = row.getLong(row.fieldIndex("offset"));
            String key          = row.isNullAt(row.fieldIndex("messageKey"))
                                    ? null : row.getString(row.fieldIndex("messageKey"));
            String typeStr      = row.getString(row.fieldIndex("messageType"));
            String rawValueB64  = row.getString(row.fieldIndex("rawValueB64"));

            try {
                byte[]      rawBytes    = Base64.getDecoder().decode(rawValueB64);
                MessageType messageType = MessageType.valueOf(typeStr);

                recordProcessor.process(rawBytes, messageType, topic, partition, offset, key);
                succeeded++;
            } catch (Exception e) {
                failed++;
                String err = "offset=%d partition=%d error=%s".formatted(offset, partition, e.getMessage());
                log.error("Replay failed at {}:{}:{} — {}", topic, partition, offset, e.getMessage(), e);
                if (errors.size() < 20) errors.add(err);
            }
        }

        log.info("Replay complete: total={} succeeded={} failed={}", totalFound, succeeded, failed);
        return new ReplayResult(totalFound, succeeded, failed, List.copyOf(errors));
    }
}
