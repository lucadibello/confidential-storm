package ch.usi.inf.confidentialstorm.enclave.crypto.aad;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Specification for Additional Authenticated Data (AAD) used in authenticated encryption.
 * It contains attributes and source/destination component information.
 */
public final class AADSpecification {
    private final Map<String, Object> attributes;
    private final TopologySpecification.Component sourceComponent;
    private final TopologySpecification.Component destinationComponent;

    /**
     * Constructs a new AADSpecification.
     *
     * @param attributes           a map of attributes to include in the AAD
     * @param sourceComponent      the source component of the encrypted data
     * @param destinationComponent the intended destination component
     */
    public AADSpecification(Map<String, Object> attributes,
                            TopologySpecification.Component sourceComponent,
                            TopologySpecification.Component destinationComponent) {
        this.attributes = attributes;
        this.sourceComponent = sourceComponent;
        this.destinationComponent = destinationComponent;
    }

    /**
     * Returns an empty AADSpecification.
     *
     * @return the empty specification
     */
    public static AADSpecification empty() {
        return new AADSpecification(Collections.emptyMap(), null, null);
    }

    /**
     * Returns a new builder for creating an AADSpecification.
     *
     * @return the builder instance
     */
    public static AADSpecificationBuilder builder() {
        return new AADSpecificationBuilder();
    }

    /**
     * Gets the attributes.
     *
     * @return the attributes map
     */
    public Map<String, Object> attributes() {
        return attributes;
    }

    /**
     * Gets the source component.
     *
     * @return an Optional containing the source component, or empty if not set
     */
    public Optional<TopologySpecification.Component> sourceComponent() {
        return Optional.ofNullable(sourceComponent);
    }

    /**
     * Gets the destination component.
     *
     * @return an Optional containing the destination component, or empty if not set
     */
    public Optional<TopologySpecification.Component> destinationComponent() {
        return Optional.ofNullable(destinationComponent);
    }

    /**
     * Checks if the specification is empty (no attributes and no components set).
     *
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return attributes.isEmpty() && sourceComponent == null && destinationComponent == null;
    }
}
