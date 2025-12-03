package ch.usi.inf.confidentialstorm.common.topology;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Declarative description of the WordCount topology used to derive routing
 * information for confidential components.
 */
public final class TopologySpecification {

    private static final TopologyProvider provider;

    static {
        ServiceLoader<TopologyProvider> loader = ServiceLoader.load(TopologyProvider.class);
        TopologyProvider found = null;
        for (TopologyProvider p : loader) {
            found = p;
            break;
        }

        // Default fallback: empty topology
        provider = Objects.requireNonNullElseGet(found, () -> component -> Collections.emptyList());
    }

    private TopologySpecification() {
    }

    public static List<Component> downstream(Component component) {
        Objects.requireNonNull(component, "componentId cannot be null");
        return provider.getDownstream(component);
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

    public enum Component implements Serializable {
        DATASET("_DATASET"),
        MAPPER("_MAPPER"),
        RANDOM_JOKE_SPOUT("random-joke-spout"),
        SENTENCE_SPLIT("sentence-split"),
        USER_CONTRIBUTION_BOUNDING("user-contribution-bounding"),
        WORD_COUNT("word-count"),
        HISTOGRAM_GLOBAL("histogram-global");

        private static final long serialVersionUID = 1L;
        private final String name;

        Component(String name) {
            this.name = name;
        }

        public static Component fromValue(String value) {
            if (value == null) {
                return null;
            }
            for (Component component : Component.values()) {
                if (component.name.equals(value) || component.name().equalsIgnoreCase(value)) {
                    return component;
                }
            }
            throw new IllegalArgumentException("Unknown component: " + value);
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
