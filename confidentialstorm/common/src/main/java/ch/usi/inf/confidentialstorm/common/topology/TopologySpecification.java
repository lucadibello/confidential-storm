package ch.usi.inf.confidentialstorm.common.topology;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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
        // Secure loading: Direct instantiation of the EncryptedTopologyProvider
        // avoiding ServiceLoader which can be hijacked via classpath manipulation.
        provider = new EncryptedTopologyProvider();
    }

    private TopologySpecification() {
    }

    /**
     * Returns the components that send tuples directly to {@code component}
     * (immediate predecessors in the topology DAG).
     *
     * @param component the component whose upstream neighbours are requested
     * @return the list of upstream components; empty if none are registered
     */
    public static List<Component> upstream(Component component) {
        Objects.requireNonNull(component, "component cannot be null");
        return provider.getUpstream(component);
    }

    /**
     * Requires that {@code component} has exactly one upstream component and returns it.
     * <p>
     * Suitable for bolts in a linear pipeline where exactly one predecessor is expected.
     * Override {@code expectedSourceComponent()} if the bolt has multiple possible sources.
     *
     * @param component the component whose single upstream neighbour is requested
     * @return the single upstream component
     * @throws IllegalArgumentException if no upstream component is configured for {@code component}
     * @throws IllegalStateException    if multiple upstream components are configured (fan-in)
     */
    public static Component requireSingleUpstream(Component component) {
        List<Component> upstream = upstream(component);
        if (upstream.isEmpty()) {
            throw new IllegalArgumentException("No upstream component configured for " + component);
        }
        if (upstream.size() > 1) {
            throw new IllegalStateException("Component " + component + " fan-in is ambiguous");
        }
        return upstream.get(0);
    }

    /**
     * Returns the components that receive tuples directly from {@code component}
     * (immediate successors in the topology DAG).
     *
     * @param component the source component
     * @return the list of downstream components; empty if none are registered
     */
    public static List<Component> downstream(Component component) {
        Objects.requireNonNull(component, "component cannot be null");
        return provider.getDownstream(component);
    }

    /**
     * Requires that {@code component} has exactly one downstream component and returns it.
     * <p>
     * Suitable for bolts in a linear pipeline where exactly one successor is expected.
     * Override {@code expectedDestinationComponent()} if the bolt fans out to multiple targets.
     *
     * @param component the source component
     * @return the single downstream component
     * @throws IllegalArgumentException if no downstream component is configured for {@code component}
     * @throws IllegalStateException    if multiple downstream components are configured (fan-out)
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
