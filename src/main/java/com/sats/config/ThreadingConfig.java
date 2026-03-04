package com.sats.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Wires the executor infrastructure described in Section 9 of the design document.
 * Virtual threads are enabled globally via {@code spring.threads.virtual.enabled=true}.
 * Spark writes use a separate bounded platform-thread pool to avoid virtual-thread
 * carrier pinning inside Spark's {@code synchronized} blocks.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class ThreadingConfig {

    private final ThreadingProperties threadingProps;

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

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("sats-scheduler-");
        return scheduler;
    }
}
