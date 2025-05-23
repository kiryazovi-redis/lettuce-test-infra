package io.lettuce.scenario.workload;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Async workload generator that performs basic Redis operations.
 */
public class AsyncWorkloadGenerator implements WorkloadGenerator {
    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> async;
    private final WorkloadStats stats;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean running;
    private ScheduledFuture<?> workloadFuture;

    public AsyncWorkloadGenerator(StatefulRedisConnection<String, String> connection) {
        this.connection = connection;
        this.async = connection.async();
        this.stats = new WorkloadStats();
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.running = new AtomicBoolean(false);
    }

    @Override
    public CompletableFuture<Void> start() {
        if (running.compareAndSet(false, true)) {
            workloadFuture = executor.scheduleWithFixedDelay(this::performOperation, 0, 1, TimeUnit.MILLISECONDS);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stop() {
        if (running.compareAndSet(true, false)) {
            if (workloadFuture != null) {
                workloadFuture.cancel(false);
            }
            executor.shutdown();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<WorkloadResult> run(Duration duration) {
        CompletableFuture<WorkloadResult> result = new CompletableFuture<>();
        
        start().thenRun(() -> {
            executor.schedule(() -> {
                stop().thenRun(() -> {
                    result.complete(new WorkloadResult(duration, stats));
                });
            }, duration.toMillis(), TimeUnit.MILLISECONDS);
        });

        return result;
    }

    @Override
    public WorkloadStats getStats() {
        return stats;
    }

    private void performOperation() {
        if (!running.get()) {
            return;
        }

        String key = "key:" + ThreadLocalRandom.current().nextInt(1000);
        String value = "value:" + System.nanoTime();

        async.set(key, value)
            .thenCompose(setResult -> async.get(key))
            .thenAccept(result -> stats.recordSuccess())
            .exceptionally(error -> {
                stats.recordError(error);
                return null;
            });
    }
} 