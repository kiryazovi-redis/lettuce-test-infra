package io.lettuce.scenario.workload;

import java.time.Duration;

/**
 * Result of a workload execution including duration and statistics.
 */
public class WorkloadResult {
    private final Duration duration;
    private final WorkloadStats stats;

    public WorkloadResult(Duration duration, WorkloadStats stats) {
        this.duration = duration;
        this.stats = stats;
    }

    public Duration getDuration() {
        return duration;
    }

    public WorkloadStats getStats() {
        return stats;
    }

    public double getOperationsPerSecond() {
        double seconds = duration.toMillis() / 1000.0;
        return seconds == 0 ? 0 : stats.getOperationsCompleted() / seconds;
    }

    @Override
    public String toString() {
        return String.format(
            "WorkloadResult{duration=%s, completed=%d, failed=%d, ops/sec=%.2f, error_rate=%.2f%%}",
            duration,
            stats.getOperationsCompleted(),
            stats.getOperationsFailed(),
            getOperationsPerSecond(),
            stats.getErrorRate() * 100
        );
    }
} 