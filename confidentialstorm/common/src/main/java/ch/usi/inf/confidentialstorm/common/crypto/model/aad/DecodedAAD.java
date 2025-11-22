package ch.usi.inf.confidentialstorm.common.crypto.model.aad;

import ch.usi.inf.confidentialstorm.common.crypto.util.AADUtils;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

import java.util.*;

public final class DecodedAAD {
    private static final DecodedAAD EMPTY =
            new DecodedAAD(Collections.emptyMap(), null, null);

    private final Map<String, Object> attributes;
    private final String sourceHash;
    private final String destinationHash;

    private DecodedAAD(Map<String, Object> attributes,
                       String sourceHash,
                       String destinationHash) {
        this.attributes = attributes;
        this.sourceHash = sourceHash;
        this.destinationHash = destinationHash;
    }

    public static DecodedAAD fromBytes(byte[] aadBytes) {
        if (aadBytes == null || aadBytes.length == 0) {
            return EMPTY;
        }
        Map<String, Object> parsed = AADUtils.parseAadJson(aadBytes);
        Object source = parsed.remove("source");
        Object destination = parsed.remove("destination");
        Map<String, Object> attrs = Collections.unmodifiableMap(new LinkedHashMap<>(parsed));
        return new DecodedAAD(attrs, toStringValue(source, "source"), toStringValue(destination, "destination"));
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    public Optional<String> sourceHash() {
        return Optional.ofNullable(sourceHash);
    }

    public Optional<String> destinationHash() {
        return Optional.ofNullable(destinationHash);
    }

    public boolean matchesSource(TopologySpecification.Component component, byte[] nonce) {
        Objects.requireNonNull(component, "Component cannot be null");
        Objects.requireNonNull(nonce, "Nonce cannot be null");
        if (sourceHash == null) {
            return false;
        }
        return sourceHash.equals(AADUtils.privatizeComponentName(component.getName(), nonce));
    }

    public void requireSource(TopologySpecification.Component component, byte[] nonce) {
        if (sourceHash == null) {
            throw new IllegalArgumentException("AAD missing source component");
        }
        if (!matchesSource(component, nonce)) {
            throw new IllegalArgumentException("AAD source mismatch for " + component.getName());
        }
    }

    public boolean matchesDestination(TopologySpecification.Component component, byte[] nonce) {
        Objects.requireNonNull(component, "Component cannot be null");
        Objects.requireNonNull(nonce, "Nonce cannot be null");
        if (destinationHash == null) {
            return false;
        }
        return destinationHash.equals(AADUtils.privatizeComponentName(component.getName(), nonce));
    }

    public void requireDestination(TopologySpecification.Component component, byte[] nonce) {
        if (destinationHash == null) {
            throw new IllegalArgumentException("AAD missing destination component");
        }
        if (!matchesDestination(component, nonce)) {
            throw new IllegalArgumentException("AAD destination mismatch for " + component.getName());
        }
    }

    private static String toStringValue(Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (value instanceof String str) {
            return str;
        }
        throw new IllegalArgumentException("AAD field '" + fieldName + "' must be a string");
    }
}
