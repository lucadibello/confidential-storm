package ch.usi.inf.confidentialstorm.common.crypto.model.aad;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class AADSpecificationBuilder {
    // NOTE: LinkedHashMap is used to preserve insertion order (necessary to generate consistent AAD byte representation)
    private final Map<String, Object> attributes = new LinkedHashMap<>();
    private String sourceComponent;
    private String destinationComponent;

    public AADSpecificationBuilder put(String key, Object value) {
        Objects.requireNonNull(key, "AAD key cannot be null");
        if ("source".equals(key) || "destination".equals(key)) {
            throw new IllegalArgumentException("AAD key '" + key + "' is reserved");
        }
        attributes.put(key, value);
        return this;
    }

    public AADSpecificationBuilder putAll(Map<String, Object> fields) {
        if (fields == null || fields.isEmpty()) {
            return this;
        }
        fields.forEach(this::put);
        return this;
    }

    public AADSpecificationBuilder sourceComponent(@NonNull  String componentName) {
        this.sourceComponent = Objects.requireNonNull(componentName, "Component name cannot be null");
        return this;
    }

    public AADSpecificationBuilder sourceComponent(@NonNull Class<?> componentType) {
        Objects.requireNonNull(componentType, "Component type cannot be null");
        return sourceComponent(componentType.getName());
    }

    public AADSpecificationBuilder destinationComponent(@NonNull String componentName) {
        this.destinationComponent = Objects.requireNonNull(componentName, "Component name cannot be null");
        return this;
    }

    public AADSpecificationBuilder destinationComponent(@NonNull Class<?> componentType) {
        Objects.requireNonNull(componentType, "Component type cannot be null");
        return destinationComponent(componentType.getName());
    }

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
