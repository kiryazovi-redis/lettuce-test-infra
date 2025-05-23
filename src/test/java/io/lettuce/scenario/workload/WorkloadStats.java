package io.lettuce.scenario.workload;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Statistics collected during workload execution.
 */
public class WorkloadStats {
    private final AtomicLong operationsCompleted;
    private final AtomicLong operationsFailed;
    private final List<Throwable> errors;

    public WorkloadStats() {
        this.operationsCompleted = new AtomicLong();
        this.operationsFailed = new AtomicLong();
        this.errors = new CopyOnWriteArrayList<>();
    }

    public void recordSuccess() {
        operationsCompleted.incrementAndGet();
    }

    public void recordError(Throwable error) {
        operationsFailed.incrementAndGet();
        errors.add(error);
    }

    public long getOperationsCompleted() {
        return operationsCompleted.get();
    }

    public long getOperationsFailed() {
        return operationsFailed.get();
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    public double getErrorRate() {
        long total = operationsCompleted.get() + operationsFailed.get();
        return total == 0 ? 0 : (double) operationsFailed.get() / total;
    }
} 