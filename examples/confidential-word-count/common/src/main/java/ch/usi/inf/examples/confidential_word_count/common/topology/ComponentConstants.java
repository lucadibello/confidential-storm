package ch.usi.inf.examples.confidential_word_count.common.topology;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

public final class ComponentConstants {
    public static final TopologySpecification.Component SPOUT_RANDOM_JOKE = TopologySpecification.Component.of("random-joke-spout");
    public static final TopologySpecification.Component BOLT_SENTENCE_SPLIT = TopologySpecification.Component.of("bolt-sentence-split");
    public static final TopologySpecification.Component BOLT_USER_CONTRIBUTION_BOUNDING = TopologySpecification.Component.of("bolt-user-contribution-bounding");
    public static final TopologySpecification.Component BOLT_DATA_PERTURBATION = TopologySpecification.Component.of("bolt-data-perturbation");
    public static final TopologySpecification.Component BOLT_HISTOGRAM_AGGREGATION = TopologySpecification.Component.of("bolt-histogram-aggregation");
    public static final TopologySpecification.Component TUNNEL_SPLIT_BOUND_SENDER = TopologySpecification.Component.of("tunnel-split-bound-sender");
    public static final TopologySpecification.Component TUNNEL_SPLIT_BOUND_RECEIVER = TopologySpecification.Component.of("tunnel-split-bound-receiver");
    public static final TopologySpecification.Component _DATASET = TopologySpecification.Component.of("_DATASET");

    private ComponentConstants() {
    }
}
