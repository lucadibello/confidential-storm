package ch.usi.inf.examples.synthetic_baseline.profiling;

import java.util.concurrent.atomic.LongAdder;

/**
 * Per-operation statistics accumulator tracking total count, avg, min, and max latency.
 * Thread-safe: {@link #record(long)} is synchronized (only called on sampled invocations).
 */
public final class EcallStats {

    private final String name;
    private final LongAdder totalCount = new LongAdder();
    private int sampledCount = 0;
    private long sumNanos = 0;
    private long minNanos = Long.MAX_VALUE;
    private long maxNanos = Long.MIN_VALUE;

    public EcallStats(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Increments the total (unsampled) invocation count.
     * Lock-free via {@link LongAdder}.
     */
    public void incrementTotal() {
        totalCount.increment();
    }

    public long getTotalCount() {
        return totalCount.sum();
    }

    /**
     * Records a sampled timing measurement.
     */
    public synchronized void record(long durationNanos) {
        sampledCount++;
        sumNanos += durationNanos;
        if (durationNanos < minNanos) minNanos = durationNanos;
        if (durationNanos > maxNanos) maxNanos = durationNanos;
    }

    public synchronized int getSampledCount() {
        return sampledCount;
    }

    public synchronized double getAvgNanos() {
        return sampledCount == 0 ? 0.0 : (double) sumNanos / sampledCount;
    }

    public synchronized long getMinNanos() {
        return sampledCount == 0 ? 0 : minNanos;
    }

    public synchronized long getMaxNanos() {
        return sampledCount == 0 ? 0 : maxNanos;
    }

    /**
     * Resets all statistics for a new reporting window.
     * Total count is NOT reset (cumulative count).
     */
    public synchronized void reset() {
        sampledCount = 0;
        sumNanos = 0;
        minNanos = Long.MAX_VALUE;
        maxNanos = Long.MIN_VALUE;
    }

    /**
     * Returns a human-readable one-line summary.
     */
    public synchronized String toSummaryLine() {
        return String.format(
                "%s: total=%d sampled=%d avg=%.2fms min=%.2fms max=%.2fms",
                name, totalCount.sum(), sampledCount,
                nanosToMs(getAvgNanos()),
                nanosToMs(getMinNanos()),
                nanosToMs(getMaxNanos()));
    }

    private static double nanosToMs(double nanos) {
        return nanos / 1_000_000.0;
    }

    private static double nanosToMs(long nanos) {
        return nanos / 1_000_000.0;
    }
}
