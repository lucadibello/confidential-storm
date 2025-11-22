package ch.usi.inf.confidentialstorm.common.topology;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Declarative description of the WordCount topology used to derive routing
 * information for confidential components.
 */
public final class TopologySpecification {

    private TopologySpecification() {
    }


    public enum Component {
        DATASET("_DATASET"),
        MAPPER("_MAPPER"),
        RANDOM_JOKE_SPOUT("random-joke-spout"),
        SENTENCE_SPLIT("sentence-split"),
        WORD_COUNT("word-count"),
        HISTOGRAM_GLOBAL("histogram-global");

        private final String name;
        Component(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final Map<Component, List<Component>> DOWNSTREAM = Map.of(
            Component.RANDOM_JOKE_SPOUT, List.of(Component.SENTENCE_SPLIT),
            Component.SENTENCE_SPLIT, List.of(Component.WORD_COUNT),
            Component.WORD_COUNT, List.of(Component.HISTOGRAM_GLOBAL),
            Component.HISTOGRAM_GLOBAL, Collections.emptyList()
    );

    public static List<Component> downstream(Component component) {
        Objects.requireNonNull(component, "componentId cannot be null");
        return DOWNSTREAM.getOrDefault(component, Collections.emptyList());
    }

    public static Component requireSingleDownstream(Component component) {
        List<Component> downstream = downstream(component);
        if (downstream.isEmpty()) {
            throw new IllegalArgumentException("No downstream component configured for " + component);
        }
        if (downstream.size() > 1) {
            throw new IllegalStateException("Component " + component + " fan-out is ambiguous");
        }
        return downstream.get(0);
    }
}
