package ch.usi.inf.examples.confidential_word_count.common.config;

/**
 * Differential privacy settings
 */
public final class DPConfig {
    /**
     * Privacy budget for the streaming histogram (Algorithm 2 in the paper).
     * <p>
     * NOTE: for the example application, we set epsilon = 10.0 to reduce noise
     * and ensure better utility (signal-to-noise ratio) for the demonstration.
     */
    public static final double EPSILON = 10.0;
    /**
     * Failure probability (delta) for the (epsilon, delta)-DP guarantee.
     * <p>
     * NOTE: for the example application, we set delta = 1e-5 as a common
     * choice in the literature for practical DP applications.
     */
    public static final double DELTA = 1e-5;

    // Privacy Budget Split (50% for Key Selection, 50% for Histogram)
    public static final double EPSILON_K = EPSILON / 2.0;
    public static final double DELTA_K = (2.0 / 3.0) * DELTA;

    public static final double EPSILON_H = EPSILON / 2.0;
    public static final double DELTA_H = DELTA / 3.0;

    // Threshold for key selection (Algorithm 3 in the paper)
    public static final long MU = 10L; // FIXME: tune this value

    /**
     * Maximum number of triggering steps supported by the DP tree.
     * This upper bounds the depth of the binary aggregation tree.
     * <p>
     * NOTE: for the example application, we set this to 60 as the local cluster
     * will run for 120 seconds, and we have a triggering interval of 2 seconds:
     * -> 120 / 2 = 60 time steps.
     */
    public static final int MAX_TIME_STEPS = 60;
    /**
     * User-level contribution bounds (Section 3.2 of the paper):
     * each user can contribute at most this many records overall.
     * <p>
     * <p>
     * NOTE: Set to 50 to balance Bias vs Variance.
     * While the 99th percentile is ~4600 words/user, high sensitivity
     * creates too much noise (sigma ~ 4600/epsilon) reducing drastically
     * the utility of the results.
     */
    public static final long MAX_CONTRIBUTIONS_PER_USER = 50L;
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

    private DPConfig() {
    }

    /**
     * Returns the user-level L1 sensitivity C * L_m, used by the DP tree to
     * calibrate Gaussian noise.
     */
    public static double l1Sensitivity() {
        return MAX_CONTRIBUTIONS_PER_USER * PER_RECORD_CLAMP;
    }
}
