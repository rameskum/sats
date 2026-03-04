package com.sats.writer;

import com.sats.domain.model.BatchPayload;
import com.sats.domain.strategy.WriteStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Factory/resolver for {@link WriteStrategy} implementations.
 * Selects the strategy matching the dataset's {@code targetFormat} (resolved
 * per-dataset from the schema registry) and delegates the write.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class WriteProvider {

    private final List<WriteStrategy> strategies;

    public void write(BatchPayload payload) {
        var targetFormat = payload.targetFormat();

        strategies.stream()
                .filter(s -> s.supports() == targetFormat)
                .findFirst()
                .ifPresentOrElse(
                        strategy -> strategy.write(payload),
                        () -> log.error("No WriteStrategy registered for target format: {}", targetFormat)
                );
    }
}
