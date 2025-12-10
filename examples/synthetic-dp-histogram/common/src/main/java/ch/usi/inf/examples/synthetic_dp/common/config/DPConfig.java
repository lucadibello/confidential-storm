package ch.usi.inf.examples.synthetic_dp.common.config;

/**
 * Defines the differential privacy parameters and contribution bounding settings for the synthetic benchmark.
 * <p>
 * Refer to section 5.1 of the paper "Differentially Private Stream Processing at Scale" for more details.
 */
public final class DPConfig {
    /**
     * Private constructor to prevent instantiation.
     */
    private DPConfig() {
    }

    /**
     * Privacy budget for the (epsilon, delta)-DP guarantee.
     */
    public static final double EPSILON = 6.0;

    /**
     * Failure probability for the (epsilon, delta)-DP guarantee.
     */
    public static final double DELTA = 1e-9;

    // Privacy Budget Split (50% for Key Selection, 50% for Histogram)
    public static final double EPSILON_K = EPSILON / 2.0;
    public static final double DELTA_K = (2.0 / 3.0) * DELTA;
    public static final double EPSILON_H = EPSILON / 2.0;
    public static final double DELTA_H = DELTA / 3.0;

    /**
     * The minimum number of unique user contributions required for releasing a key in the histogram.
     */
    public static final long MU = Long.getLong("dp.mu", 50L);

    /**
     * The maximum number of time steps to consider for the data stream.
     */
    public static final int MAX_TIME_STEPS = Integer.getInteger("dp.max.time.steps", 100);

    // Contribution bounding (C, L_m)

    /**
     * The maximum number of contributions a single user can make across all time steps.
     */
    public static final long MAX_CONTRIBUTIONS_PER_USER = 32L;

    /**
     * The maximum absolute value for each individual record contribution.
     */
    public static final double PER_RECORD_CLAMP = 1.0;

    /**
     * Computes the L1 sensitivity based on the contribution bounding parameters.
     * @return the L1 sensitivity.
     */
    public static double l1Sensitivity() {
        return MAX_CONTRIBUTIONS_PER_USER * PER_RECORD_CLAMP;
    }

    /**
     * Returns a string description of the DP configuration.
     * @return a string describing the DP configuration.
     */
    public static String describe() {
        return "DPConfig{" +
                "EPSILON=" + EPSILON +
                ", DELTA=" + DELTA +
                ", MAX_CONTRIBUTIONS_PER_USER=" + MAX_CONTRIBUTIONS_PER_USER +
                ", PER_RECORD_CLAMP=" + PER_RECORD_CLAMP +
                ", MU=" + MU +
                ", MAX_TIME_STEPS=" + MAX_TIME_STEPS +
                '}';
    }
}
