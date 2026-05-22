package ch.usi.inf.examples.synthetic_dp.common.config;

/**
 * Defines the differential privacy parameters and contribution bounding settings for the synthetic benchmark.
 * <p>
 * Refer to section 5.1 of the paper "Differentially Private Stream Processing at Scale" for more details.
 * <p>
 * Runtime-configurable values (MU, MAX_TIME_STEPS, PARALLELISM) are read from system properties
 * via getter methods so that {@link System#setProperty} calls in the topology's {@code main()}
 * take effect before the values are read.
 */
public final class DPConfig {

    /**
     * Private constructor to prevent instantiation.
     */
    private DPConfig() {}

    /**
     * Privacy budget for the (epsilon, delta)-DP guarantee.
     */
    public static final double EPSILON = 6.0;

    /**
     * Failure probability for the (epsilon, delta)-DP guarantee.
     */
    public static final double DELTA = 1e-9;

    /**
     * The maximum number of contributions a single user can make across all time steps.
     */
    public static final long MAX_CONTRIBUTIONS_PER_USER = 32L;

    /**
     * The maximum absolute value for each individual record contribution.
     * NOTE: we set L=1 since the aggregation function is COUNT.
     */
    public static final double PER_RECORD_CLAMP = 1.0;

    /**
     * The minimum number of unique user contributions required for releasing a key in the histogram.
     * Configurable via {@code -Ddp.mu=<value>} or {@code --mu <value>}.
     */
    public static long mu() {
        return Long.getLong("dp.mu", 50L);
    }

    /**
     * The maximum number of time steps to consider for the data stream.
     * Configurable via {@code -Ddp.max.time.steps=<value>} or {@code --max-time-steps <value>}.
     */
    public static int maxTimeSteps() {
        return Integer.getInteger("dp.max.time.steps", 100);
    }

    /**
     * Parallelism hint for the bounding and perturbation bolts.
     * Default is 8 (benchmark mode, designed for 32-CPU servers).
     * Configurable via {@code -Dsynthetic.parallelism=<value>} or {@code --parallelism <value>}
     * or {@code --test} flag (sets to 1).
     */
    public static int parallelism() {
        return Integer.getInteger("synthetic.parallelism", 4);
    }

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
        return (
            "DPConfig{" +
            "EPSILON=" +
            EPSILON +
            ", DELTA=" +
            DELTA +
            ", MAX_CONTRIBUTIONS_PER_USER=" +
            MAX_CONTRIBUTIONS_PER_USER +
            ", PER_RECORD_CLAMP=" +
            PER_RECORD_CLAMP +
            ", MU=" +
            mu() +
            ", MAX_TIME_STEPS=" +
            maxTimeSteps() +
            ", PARALLELISM=" +
            parallelism() +
            '}'
        );
    }
}
