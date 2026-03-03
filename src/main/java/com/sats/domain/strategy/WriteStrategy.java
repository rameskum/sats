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
