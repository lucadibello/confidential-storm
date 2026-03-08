package ch.usi.inf.confidentialstorm.host.profiling;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per-ECALL-type statistics accumulator with a fixed-size ring buffer for percentile computation.
 * Thread-safe: {@link #record(long)} is synchronized (only called on sampled invocations).
 */
public final class EcallStats {

    private final String name;
    private final LongAdder totalCount = new LongAdder();
    private final long[] reservoir;
    private int reservoirIndex = 0;
    private int sampledCount = 0;
    private long sumNanos = 0;
    private long minNanos = Long.MAX_VALUE;
    private long maxNanos = Long.MIN_VALUE;

    public EcallStats(String name) {
        this.name = name;
        this.reservoir = new long[ProfilerConfig.RESERVOIR_SIZE];
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
        reservoir[reservoirIndex % reservoir.length] = durationNanos;
        reservoirIndex++;
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
     * Returns the requested percentile (0.0–1.0) from the reservoir.
     */
    public synchronized long getPercentile(double p) {
        int count = Math.min(sampledCount, reservoir.length);
        if (count == 0) return 0;
        long[] sorted = Arrays.copyOf(reservoir, count);
        Arrays.sort(sorted);
        int idx = Math.min((int) (p * count), count - 1);
        return sorted[idx];
    }

    /**
     * Resets all statistics for a new reporting window.
     * Total count is NOT reset (cumulative count).
     */
    public synchronized void reset() {
        reservoirIndex = 0;
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
                "%s: total=%d sampled=%d avg=%.2fms p50=%.2fms p95=%.2fms p99=%.2fms min=%.2fms max=%.2fms",
                name, totalCount.sum(), sampledCount,
                nanosToMs(getAvgNanos()),
                nanosToMs(getPercentile(0.50)),
                nanosToMs(getPercentile(0.95)),
                nanosToMs(getPercentile(0.99)),
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
