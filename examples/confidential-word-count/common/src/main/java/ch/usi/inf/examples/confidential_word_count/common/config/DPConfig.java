package ch.usi.inf.examples.confidential_word_count.common.config;

/**
 * Differential privacy settings
 */
public final class DPConfig {
    private DPConfig() {
    }

    /**
     * Privacy budget for the streaming histogram (Algorithm 2 in the paper).
     * <p>
     * NOTE: for the example application, we set epsilon = 1.0 as a reasonable
     * trade-off between privacy and utility.
     */
    public static final double EPSILON = 1.0;

    /**
     * Failure probability (delta) for the (epsilon, delta)-DP guarantee.
     * <p>
     * NOTE: for the example application, we set delta = 1e-5 as a common
     * choice in the literature for practical DP applications.
     */
    public static final double DELTA = 1e-5;

    /**
     * Maximum number of triggering steps supported by the DP tree.
     * This upper bounds the depth of the binary aggregation tree.
     * <p>
     * NOTE: for the example application, we set this to 24 as the local cluster
     * will run for 120 seconds, and we have a triggering interval of 5 seconds:
     * -> 120 / 5 = 24 time steps.
     */
    public static final int MAX_TIME_STEPS = 24;

    /**
     * User-level contribution bounds (Section 3.2 of the paper):
     * each user can contribute at most this many records overall.
     * <p>
     * NOTE:  Based on the * per-user percentile estimator (p=0.99)
     * on the current dataset, * a bound of ~4600 words avoids
     * dropping ~99% of users.
     */
    public static final long MAX_CONTRIBUTIONS_PER_USER = 4600L;

    /**
     * Per-record clamp for the value being aggregated (|v| <= L_m).
     * <p>
     * NOTE: for this confidential-word-count example we set this is 1 because
     * each record contributes a single count.
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
