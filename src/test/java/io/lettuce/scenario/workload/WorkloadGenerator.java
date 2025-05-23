package io.lettuce.scenario.workload;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for workload generators that can run Redis operations
 * while collecting statistics and errors.
 */
public interface WorkloadGenerator {

    /**
     * Start the workload generator.
     * @return A future that completes when the generator has started
     */
    CompletableFuture<Void> start();

    /**
     * Stop the workload generator.
     * @return A future that completes when the generator has stopped
     */
    CompletableFuture<Void> stop();

    /**
     * Run the workload for a specific duration.
     * @param duration How long to run the workload
     * @return A future that completes when the duration has elapsed
     */
    CompletableFuture<WorkloadResult> run(Duration duration);

    /**
     * Get the current statistics from the workload.
     * @return The current workload statistics
     */
    WorkloadStats getStats();
} 