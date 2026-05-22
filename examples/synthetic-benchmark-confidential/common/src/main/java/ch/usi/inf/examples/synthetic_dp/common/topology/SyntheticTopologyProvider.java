package ch.usi.inf.examples.synthetic_dp.common.topology;

import ch.usi.inf.confidentialstorm.common.topology.TopologyProvider;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification.Component;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class SyntheticTopologyProvider implements TopologyProvider {

    private static final Map<Component, List<Component>> DOWNSTREAM = Map.of(
            ComponentConstants.SPOUT, List.of(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING),
            ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING, List.of(ComponentConstants.BOLT_DATA_PERTURBATION),
            ComponentConstants.BOLT_DATA_PERTURBATION, List.of(ComponentConstants.BOLT_HISTOGRAM_AGGREGATION),
            ComponentConstants.BOLT_HISTOGRAM_AGGREGATION, Collections.emptyList()
    );

    private static final Map<Component, List<Component>> UPSTREAM = Map.of(
            ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING, List.of(ComponentConstants.SPOUT),
            ComponentConstants.BOLT_DATA_PERTURBATION, List.of(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING),
            ComponentConstants.BOLT_HISTOGRAM_AGGREGATION, List.of(ComponentConstants.BOLT_DATA_PERTURBATION)
    );

    @Override
    public List<Component> getDownstream(Component component) {
        return DOWNSTREAM.getOrDefault(component, Collections.emptyList());
    }

    @Override
    public List<Component> getUpstream(Component component) {
        return UPSTREAM.getOrDefault(component, Collections.emptyList());
    }
}
