package ch.usi.inf.confidentialstorm.enclave.crypto.aad;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for creating {@link AADSpecification} instances.
 */
public final class AADSpecificationBuilder {
    // NOTE: LinkedHashMap is used to preserve insertion order (necessary to generate consistent AAD byte representation)
    private final Map<String, Object> attributes = new LinkedHashMap<>();
    private TopologySpecification.Component sourceComponent;
    private TopologySpecification.Component destinationComponent;

    /**
     * Adds an attribute to the AAD.
     *
     * @param key   the attribute key (cannot be null, "source" and "destination" are reserved)
     * @param value the attribute value
     * @return this builder instance
     * @throws NullPointerException     if key is null
     * @throws IllegalArgumentException if key is reserved
     */
    public AADSpecificationBuilder put(String key, Object value) {
        Objects.requireNonNull(key, "AAD key cannot be null");
        if ("source".equals(key) || "destination".equals(key)) {
            throw new IllegalArgumentException("AAD key '" + key + "' is reserved");
        }
        attributes.put(key, value);
        return this;
    }

    /**
     * Adds all attributes from the given map to the AAD.
     *
     * @param fields the map of attributes to add
     * @return this builder instance
     */
    public AADSpecificationBuilder putAll(Map<String, Object> fields) {
        if (fields == null || fields.isEmpty()) {
            return this;
        }
        fields.forEach(this::put);
        return this;
    }

    /**
     * Sets the source component in the AAD.
     *
     * @param component the source component
     * @return this builder instance
     * @throws NullPointerException if component is null
     */
    public AADSpecificationBuilder sourceComponent(TopologySpecification.Component component) {
        this.sourceComponent = Objects.requireNonNull(component, "Component cannot be null");
        return this;
    }

    /**
     * Sets the destination component in the AAD.
     *
     * @param component the destination component
     * @return this builder instance
     * @throws NullPointerException if component is null
     */
    public AADSpecificationBuilder destinationComponent(TopologySpecification.Component component) {
        this.destinationComponent = Objects.requireNonNull(component, "Component cannot be null");
        return this;
    }

    /**
     * Builds the AADSpecification.
     *
     * @return the created AADSpecification
     */
    public AADSpecification build() {
        // if empty, return singleton instance
        if (attributes.isEmpty() && sourceComponent == null && destinationComponent == null) {
            return AADSpecification.empty();
        }
        // otherwise, create new instance
        Map<String, Object> copy = new LinkedHashMap<>(attributes);
        return new AADSpecification(Collections.unmodifiableMap(copy), sourceComponent, destinationComponent);
    }
}
