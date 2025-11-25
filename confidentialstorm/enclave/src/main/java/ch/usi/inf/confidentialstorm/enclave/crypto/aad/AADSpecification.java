package ch.usi.inf.confidentialstorm.enclave.crypto.aad;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public final class AADSpecification {
    private final Map<String, Object> attributes;
    private final TopologySpecification.Component sourceComponent;
    private final TopologySpecification.Component destinationComponent;

    public AADSpecification(Map<String, Object> attributes,
                            TopologySpecification.Component sourceComponent,
                            TopologySpecification.Component destinationComponent) {
        this.attributes = attributes;
        this.sourceComponent = sourceComponent;
        this.destinationComponent = destinationComponent;
    }

    // singleton empty instance to avoid unnecessary allocations
    public static AADSpecification empty() {
        return new AADSpecification(Collections.emptyMap(), null, null);
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    public Optional<TopologySpecification.Component> sourceComponent() {
        return Optional.ofNullable(sourceComponent);
    }

    public Optional<TopologySpecification.Component> destinationComponent() {
        return Optional.ofNullable(destinationComponent);
    }

    public boolean isEmpty() {
        return attributes.isEmpty() && sourceComponent == null && destinationComponent == null;
    }

    public static AADSpecificationBuilder builder() {
        return new AADSpecificationBuilder();
    }
}
