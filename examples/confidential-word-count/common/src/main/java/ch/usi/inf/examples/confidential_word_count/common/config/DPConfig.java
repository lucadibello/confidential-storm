package ch.usi.inf.examples.confidential_word_count.common.config;

/**
 * Differential privacy settings
 */
public final class DPConfig {
    private DPConfig() {
    }

    /**
     * Privacy budget for the streaming histogram (Algorithm 2 in the paper).
     */
    public static final double EPSILON = 1.0;

    /**
     *
     */
    public static final double DELTA = 1e-5;

    /**
     * Maximum number of triggering steps supported by the DP tree.
     * This upper bounds the depth of the binary aggregation tree.
     */
    public static final int MAX_TIME_STEPS = 2048;

    /**
     * User-level contribution bounds (Section 3.2 of the paper):
     * each user can contribute at most this many records overall.
     */
    public static final long MAX_CONTRIBUTIONS_PER_USER = 10L;

    /**
     * Per-record clamp for the value being aggregated (|v| <= L_m).
     * NOTE: for word-count this is 1 because each record contributes a single count.
     */
    public static final double PER_RECORD_CLAMP = 1.0;

    /**
     * Returns the user-level L1 sensitivity C * L_m, used by the DP tree to
     * calibrate Gaussian noise.
     */
    public static double l1Sensitivity() {
        return MAX_CONTRIBUTIONS_PER_USER * PER_RECORD_CLAMP;
    }
}
