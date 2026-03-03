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
