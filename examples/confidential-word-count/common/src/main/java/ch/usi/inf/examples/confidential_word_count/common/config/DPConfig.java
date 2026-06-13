package ch.usi.inf.examples.confidential_word_count.common.config;

/**
 * Differential privacy parameters and contribution bounding settings for the confidential word count topology.
 */
public final class DPConfig {
    /**
     * Privacy budget (epsilon) for the DP guarantee.
     */
    public static final double EPSILON = 8.0;
    /**
     * Failure probability (delta) for the DP guarantee.
     */
    public static final double DELTA = 1e-6;

    /**
     * Maximum contributions a user can make across all time steps.
     */
    public static final long MAX_CONTRIBUTIONS_PER_USER = 100L;

    /**
     * Maximum absolute value for individual record contribution.
     * Set to 1.0 since the aggregation function is COUNT.
     */
    public static final double PER_RECORD_CLAMP = 1.0;

    /**
     * Private constructor to prevent instantiation.
     */
    private DPConfig() {
    }
}
