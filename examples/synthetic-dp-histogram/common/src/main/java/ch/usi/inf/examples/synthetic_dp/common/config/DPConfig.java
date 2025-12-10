package ch.usi.inf.examples.synthetic_dp.common.config;

/**
 * Parameters aligned with the synthetic experiment (Section 5.1).
 */
public final class DPConfig {
    public static final double EPSILON = 6.0;
    public static final double DELTA = 1e-9;

    public static final double EPSILON_K = EPSILON / 2.0;
    public static final double DELTA_K = (2.0 / 3.0) * DELTA;
    public static final double EPSILON_H = EPSILON / 2.0;
    public static final double DELTA_H = DELTA / 3.0;

    // Contribution bounding (C, L_m)
    public static final long MAX_CONTRIBUTIONS_PER_USER = 32L;
    public static final double PER_RECORD_CLAMP = 1.0;

    // Pre-threshold mu
    public static final long MU = 50L;

    // Default number of time steps (matches paper: 100 or 1000 micro-batches)
    public static final int MAX_TIME_STEPS = 1000;

    private DPConfig() {
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
                ", MU=" + MU +
                ", MAX_TIME_STEPS=" + MAX_TIME_STEPS +
                '}';
    }
}
