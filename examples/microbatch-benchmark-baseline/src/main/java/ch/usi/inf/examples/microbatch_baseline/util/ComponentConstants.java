package ch.usi.inf.examples.microbatch_baseline.util;

/**
 * Topology component and stream-name constants for the micro-batch benchmark.
 */
public final class ComponentConstants {
    public static final String SPOUT = "spout";
    public static final String BOLT_USER_CONTRIBUTION_BOUNDING = "bolt-user-contribution-bounding";
    public static final String BOLT_DATA_PERTURBATION = "bolt-data-perturbation";
    public static final String BOLT_HISTOGRAM_AGGREGATION = "bolt-histogram-aggregation";

    /**
     * Side-channel stream carrying batch markers (BEGIN/END). Subscribed via
     * allGrouping so every replica observes every marker.
     */
    public static final String CONTROL_STREAM = "control";

    private ComponentConstants() {}
}
