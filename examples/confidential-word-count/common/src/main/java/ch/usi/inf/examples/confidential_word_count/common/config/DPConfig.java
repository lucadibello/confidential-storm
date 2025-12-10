package ch.usi.inf.examples.confidential_word_count.common.config;

/**
 * Defines the differential privacy parameters and contribution bounding settings for the confidential word count example.
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
    public static final double EPSILON = 4.0;

    /**
     * Failure probability (delta) for the (epsilon, delta)-DP guarantee.
     */
    public static final double DELTA = 1e-6;

    // Privacy Budget Split (50% for Key Selection, 50% for Histogram)
    public static final double EPSILON_K = EPSILON / 2.0;
    public static final double DELTA_K = (2.0 / 3.0) * DELTA;
    public static final double EPSILON_H = EPSILON / 2.0;
    public static final double DELTA_H = DELTA / 3.0;

    /**
     * The minimum number of unique user contributions required for releasing a key in the histogram.
     */
    public static final long MU = 5L;

    /**
     * The maximum number of time steps to consider for the data stream.
     * <p>
     * NOTE: * this example runs for 120 seconds, and we trigger a release every 5 seconds:
     *  120 / 5 = 24 time steps.
     */
    public static final int MAX_TIME_STEPS = 24;

    /**
     * The maximum number of contributions a single user can make across all time steps.
     */
    public static final long MAX_CONTRIBUTIONS_PER_USER = 25L;

    /**
     * The maximum absolute value for each individual record contribution.
     */
    public static final double PER_RECORD_CLAMP = 1.0;

    /**
     * Feature toggle: when true enforce user-level DP (bounding + AAD propagation of user_id);
     * when false operate in event-level mode without requiring user identifiers.
     */
    public static final boolean ENABLE_USER_LEVEL_PRIVACY = true;

    /**
     * Returns the user-level L1 sensitivity C * L_m, used by the DP tree to
     * calibrate Gaussian noise.
     */
    public static double l1Sensitivity() {
        return MAX_CONTRIBUTIONS_PER_USER * PER_RECORD_CLAMP;
    }
}
