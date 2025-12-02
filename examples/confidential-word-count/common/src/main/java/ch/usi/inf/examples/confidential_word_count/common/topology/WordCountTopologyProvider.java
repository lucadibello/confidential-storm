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
            Component.RANDOM_JOKE_SPOUT, List.of(Component.SENTENCE_SPLIT),
            Component.SENTENCE_SPLIT, List.of(Component.USER_CONTRIBUTION_BOUNDING),
            Component.USER_CONTRIBUTION_BOUNDING, List.of(Component.WORD_COUNT),
            Component.WORD_COUNT, List.of(Component.HISTOGRAM_GLOBAL),
            Component.HISTOGRAM_GLOBAL, Collections.emptyList()
    );

    @Override
    public List<Component> getDownstream(TopologySpecification.Component component) {
        return DOWNSTREAM.getOrDefault(component, Collections.emptyList());
    }
}
