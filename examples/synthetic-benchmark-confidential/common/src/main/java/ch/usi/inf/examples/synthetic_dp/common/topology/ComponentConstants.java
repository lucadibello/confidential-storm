package ch.usi.inf.examples.synthetic_dp.common.topology;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

public final class ComponentConstants {
    public static final TopologySpecification.Component SPOUT = TopologySpecification.Component.of("spout");
    public static final TopologySpecification.Component BOLT_USER_CONTRIBUTION_BOUNDING = TopologySpecification.Component.of("bolt-user-contribution-bounding");
    public static final TopologySpecification.Component BOLT_DATA_PERTURBATION = TopologySpecification.Component.of("bolt-data-perturbation");
    public static final TopologySpecification.Component BOLT_HISTOGRAM_AGGREGATION = TopologySpecification.Component.of("bolt-histogram-aggregation");

    /** Stream id used by the spout to emit plaintext ground-truth keys to the aggregation bolt. */
    public static final String GROUND_TRUTH_STREAM = "ground-truth";

    private ComponentConstants() {}
}
