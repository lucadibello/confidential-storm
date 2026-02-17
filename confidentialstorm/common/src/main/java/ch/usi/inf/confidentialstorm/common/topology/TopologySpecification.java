package ch.usi.inf.confidentialstorm.common.topology;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Declarative description of a topology used to derive routing
 * information for confidential components.
 * <p>
 * Component identifiers are no longer hard-coded; arbitrary components can be
 * registered via constants or Component.of(...).
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
        provider = Objects.requireNonNullElseGet(found, () -> component -> Collections.emptyList());
    }

    private TopologySpecification() {
    }

    /**
     * Gets the downstream components for a given component.
     *
     * @param component the source component
     * @return the list of downstream components
     */
    public static List<Component> downstream(Component component) {
        Objects.requireNonNull(component, "component cannot be null");
        return provider.getDownstream(component);
    }

    /**
     * Requires that a component has exactly one downstream component and returns it.
     *
     * @param component the source component
     * @return the single downstream component
     * @throws IllegalArgumentException if no downstream component is configured
     * @throws IllegalStateException    if multiple downstream components are configured
     */
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

    /**
     * Component identifier, flexible (not enum) to allow example-specific topologies.
     */
    public static final class Component implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final Map<String, Component> REGISTRY = new ConcurrentHashMap<>();

        private final String name;

        private Component(String name) {
            this.name = name;
        }

        /**
         * Gets or creates a Component instance with the given name.
         *
         * @param name the name of the component
         * @return the Component instance
         */
        public static Component of(String name) {
            Objects.requireNonNull(name, "component name cannot be null");
            String key = normalize(name);
            return REGISTRY.computeIfAbsent(key, k -> new Component(name));
        }

        /**
         * Creates a Component from a string value. Returns null if input is null.
         *
         * @param value the string value
         * @return the Component instance, or null
         */
        public static Component fromValue(String value) {
            if (value == null) {
                return null;
            }
            return of(value);
        }

        private static String normalize(String name) {
            return name.toLowerCase(Locale.ROOT);
        }

        /**
         * Gets the name of the component.
         *
         * @return the component name
         */
        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Component other)) return false;
            return normalize(this.name).equals(normalize(other.name));
        }

        @Override
        public int hashCode() {
            return normalize(this.name).hashCode();
        }
    }
}
