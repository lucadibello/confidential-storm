package ch.usi.inf.examples.synthetic_dp.common.topology;

import ch.usi.inf.confidentialstorm.common.topology.TopologyProvider;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification.Component;
import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@AutoService(TopologyProvider.class)
public final class SyntheticTopologyProvider implements TopologyProvider {

    private static final Map<Component, List<Component>> DOWNSTREAM = Map.of(
            Component.RANDOM_JOKE_SPOUT, List.of(Component.HISTOGRAM_GLOBAL),
            Component.HISTOGRAM_GLOBAL, Collections.emptyList()
    );

    @Override
    public List<Component> getDownstream(Component component) {
        return DOWNSTREAM.getOrDefault(component, Collections.emptyList());
    }
}
