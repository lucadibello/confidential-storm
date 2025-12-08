package ch.usi.inf.examples.synthetic_dp.common.topology;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

public final class ComponentConstants {
    public static final TopologySpecification.Component SPOUT = TopologySpecification.Component.of("spout");
    public static final TopologySpecification.Component HISTOGRAM_GLOBAL = TopologySpecification.Component.of("histogram-global");

    private ComponentConstants() {}
}
