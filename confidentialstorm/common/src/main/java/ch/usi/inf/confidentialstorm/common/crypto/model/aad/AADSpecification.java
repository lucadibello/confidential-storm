package ch.usi.inf.confidentialstorm.common.crypto.model.aad;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public final class AADSpecification {
    private static final AADSpecification EMPTY =
            new AADSpecification(Collections.emptyMap(), null, null);

    private final Map<String, Object> attributes;
    private final String sourceComponent;
    private final String destinationComponent;

    public AADSpecification(Map<String, Object> attributes,
                            String sourceComponent,
                            String destinationComponent) {
        this.attributes = attributes;
        this.sourceComponent = sourceComponent;
        this.destinationComponent = destinationComponent;
    }

    // singleton empty instance to avoid unnecessary allocations
    public static AADSpecification empty() {
        return EMPTY;
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    public Optional<String> sourceComponent() {
        return Optional.ofNullable(sourceComponent);
    }

    public Optional<String> destinationComponent() {
        return Optional.ofNullable(destinationComponent);
    }

    public boolean isEmpty() {
        return attributes.isEmpty() && sourceComponent == null && destinationComponent == null;
    }

    public static AADSpecificationBuilder builder() {
        return new AADSpecificationBuilder();
    }
}

