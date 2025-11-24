package ch.usi.inf.confidentialstorm.common.crypto.model.aad;

import ch.usi.inf.confidentialstorm.common.crypto.util.AADUtils;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

import java.util.*;

public final class DecodedAAD {
    private static final DecodedAAD EMPTY =
            new DecodedAAD(Collections.emptyMap(), null, null, null);

    private final Map<String, Object> attributes;
    private final String sourceName;
    private final String destinationName;
    private final Integer sequenceName;

    private DecodedAAD(Map<String, Object> attributes,
                       String sourceName,
                       String destinationName,
                       Integer sequenceNumber
    ) {
        this.attributes = attributes;
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.sequenceName = sequenceNumber;
    }

    public static DecodedAAD fromBytes(byte[] aadBytes) {
        if (aadBytes == null || aadBytes.length == 0) {
            return EMPTY;
        }
        Map<String, Object> parsed = AADUtils.parseAadJson(aadBytes);
        Object source = parsed.remove("source");
        Object destination = parsed.remove("destination");
        // optional: remove sequence number if present
        Object sequenceNumber = parsed.remove("sequence_number");
        Map<String, Object> attrs = Collections.unmodifiableMap(new LinkedHashMap<>(parsed));

        // construct DecodedAAD instance
        return new DecodedAAD(attrs,
                toStringValue(source, "source"),
                toStringValue(destination, "destination"),
                toIntegerValue(sequenceNumber, "sequence_number"));
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    public Optional<String> sourceHash() {
        return Optional.ofNullable(sourceName);
    }

    public Optional<String> destinationHash() {
        return Optional.ofNullable(destinationName);
    }

    public boolean matchesSource(TopologySpecification.Component component) {
        Objects.requireNonNull(component, "Component cannot be null");
        if (sourceName == null) {
            return false;
        }
        return sourceName.equals(component.getName());
    }

    public void requireSource(TopologySpecification.Component component) {
        if (sourceName == null) {
            throw new IllegalArgumentException("AAD missing source component");
        }
        if (!matchesSource(component)) {
            throw new IllegalArgumentException("AAD source mismatch for " + component.getName());
        }
    }

    public boolean matchesDestination(TopologySpecification.Component component) {
        Objects.requireNonNull(component, "Component cannot be null");
        if (destinationName == null) {
            return false;
        }
        return destinationName.equals(component.getName());
    }

    public void requireDestination(TopologySpecification.Component component) {
        if (destinationName == null) {
            throw new IllegalArgumentException("AAD missing destination component");
        }
        if (!matchesDestination(component)) {
            throw new IllegalArgumentException("AAD destination mismatch for " + component.getName());
        }
    }

    public void requireSequenceNumber(int expectedSequenceNumber) {
        if (sequenceName == null) {
            throw new IllegalArgumentException("AAD missing sequence number");
        }
        if (!sequenceName.equals(expectedSequenceNumber)) {
            throw new IllegalArgumentException("AAD sequence number mismatch: expected "
                    + expectedSequenceNumber + ", got " + sequenceName);
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

    private static Integer toIntegerValue(Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer integer) {
            return integer;
        }
        throw new IllegalArgumentException("AAD field '" + fieldName + "' must be an integer");
    }

    @Override
    public String toString() {
        return "DecodedAAD{" +
                "attributes=" + attributes +
                ", sourceHash='" + sourceName + '\'' +
                ", destinationHash='" + destinationName + '\'' +
                '}';
    }
}