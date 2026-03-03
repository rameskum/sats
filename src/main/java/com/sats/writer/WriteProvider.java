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
