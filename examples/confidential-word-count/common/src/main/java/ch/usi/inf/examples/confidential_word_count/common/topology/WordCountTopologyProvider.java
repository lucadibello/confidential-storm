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
            ComponentConstants.RANDOM_JOKE_SPOUT, List.of(ComponentConstants.SENTENCE_SPLIT),
            ComponentConstants.SENTENCE_SPLIT, List.of(ComponentConstants.USER_CONTRIBUTION_BOUNDING),
            ComponentConstants.USER_CONTRIBUTION_BOUNDING, List.of(ComponentConstants.WORD_COUNT),
            ComponentConstants.WORD_COUNT, List.of(ComponentConstants.HISTOGRAM_GLOBAL),
            ComponentConstants.HISTOGRAM_GLOBAL, Collections.emptyList()
    );

    @Override
    public List<Component> getDownstream(TopologySpecification.Component component) {
        return DOWNSTREAM.getOrDefault(component, Collections.emptyList());
    }
}
