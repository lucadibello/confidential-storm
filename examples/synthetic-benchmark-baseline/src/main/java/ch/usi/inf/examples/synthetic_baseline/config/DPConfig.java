package ch.usi.inf.examples.synthetic_baseline.config;

/**
 * Differential privacy parameters and contribution bounding settings for the synthetic benchmark.
 * (copy of synthetic-dp-histogram/common)
 */
public final class DPConfig {
    private DPConfig() {}

    public static final double EPSILON = 6.0;
    public static final double DELTA = 1e-9;
    public static final long MAX_CONTRIBUTIONS_PER_USER = 32L;
    public static final double PER_RECORD_CLAMP = 1.0;

    public static long mu() {
        return Long.getLong("dp.mu", 50L);
    }

    public static int maxTimeSteps() {
        return Integer.getInteger("dp.max.time.steps", 100);
    }

    public static int parallelism() {
        return Integer.getInteger("synthetic.parallelism", 4);
    }

    public static double l1Sensitivity() {
        return MAX_CONTRIBUTIONS_PER_USER * PER_RECORD_CLAMP;
    }

    public static String describe() {
        return "DPConfig{" +
                "EPSILON=" + EPSILON +
                ", DELTA=" + DELTA +
                ", MAX_CONTRIBUTIONS_PER_USER=" + MAX_CONTRIBUTIONS_PER_USER +
                ", PER_RECORD_CLAMP=" + PER_RECORD_CLAMP +
                ", MU=" + mu() +
                ", MAX_TIME_STEPS=" + maxTimeSteps() +
                ", PARALLELISM=" + parallelism() +
                '}';
    }
}
