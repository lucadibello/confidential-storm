package ch.usi.inf.examples.confidential_word_count.common.config;

/**
 * Defines the differential privacy parameters and contribution bounding settings for the confidential word count example.
 */
public final class DPConfig {
    /**
     * Privacy budget for the (epsilon, delta)-DP guarantee.
     */
    public static final double EPSILON = 8.0;
    /**
     * Failure probability (delta) for the (epsilon, delta)-DP guarantee.
     */
    public static final double DELTA = 1e-6;

    /**
     * The maximum number of contributions a single user can make across all time steps.
     */
    public static final long MAX_CONTRIBUTIONS_PER_USER = 100L;

    /**
     * The maximum absolute value for each individual record contribution.
     * NOTE: we set L=1 since the aggregation function is COUNT.
     */
    public static final double PER_RECORD_CLAMP = 1.0;

    /**
     * Private constructor to prevent instantiation.
     */
    private DPConfig() {
    }
}
