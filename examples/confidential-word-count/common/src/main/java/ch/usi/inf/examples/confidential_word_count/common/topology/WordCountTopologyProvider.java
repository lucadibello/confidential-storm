package ch.usi.inf.examples.confidential_word_count.common.topology;

import ch.usi.inf.confidentialstorm.common.topology.TopologyProvider;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification.Component;
import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@AutoService(TopologyProvider.class)
public class WordCountTopologyProvider implements TopologyProvider {

    private static final Map<Component, List<Component>> DOWNSTREAM = Map.of(
            ComponentConstants.SPOUT_RANDOM_JOKE, List.of(ComponentConstants.BOLT_SENTENCE_SPLIT),
            ComponentConstants.BOLT_SENTENCE_SPLIT, List.of(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING),
            ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING, List.of(ComponentConstants.BOLT_DATA_PERTURBATION),
            ComponentConstants.BOLT_DATA_PERTURBATION, List.of(ComponentConstants.BOLT_HISTOGRAM_AGGREGATION),
            ComponentConstants.BOLT_HISTOGRAM_AGGREGATION, List.of()
    );

    @Override
    public List<Component> getDownstream(TopologySpecification.Component component) {
        return DOWNSTREAM.getOrDefault(component, Collections.emptyList());
    }
}
