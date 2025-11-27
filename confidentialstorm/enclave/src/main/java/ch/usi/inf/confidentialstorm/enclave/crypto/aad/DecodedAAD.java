package ch.usi.inf.confidentialstorm.enclave.crypto.aad;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.util.AADUtils;

import java.util.*;

public final class DecodedAAD {
    private final Map<String, Object> attributes;
    private final String sourceName;
    private final String destinationName;
    private final Long sequenceNumber;
    private final String producerId;

    private DecodedAAD(Map<String, Object> attributes,
                       String sourceName,
                       String destinationName,
                       Long sequenceNumber,
                       String producerId
    ) {
        this.attributes = attributes;
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.sequenceNumber = sequenceNumber;
        this.producerId = producerId;
    }

    public static DecodedAAD fromBytes(byte[] aadBytes) {
        if (aadBytes == null || aadBytes.length == 0) {
            // empty
            return new DecodedAAD(Collections.emptyMap(), null, null, null, null);
        }
        Map<String, Object> parsed = AADUtils.parseAadJson(aadBytes);
        Object source = parsed.remove("source");
        Object destination = parsed.remove("destination");
        Object producerId = parsed.remove("producer_id");
        // optional: remove sequence number if present
        Object sequenceNumber = parsed.remove("seq");
        if (sequenceNumber == null) {
            sequenceNumber = parsed.remove("sequence_number"); // backward compatibility
        }
        Map<String, Object> attrs = Collections.unmodifiableMap(new LinkedHashMap<>(parsed));

        // construct DecodedAAD instance
        return new DecodedAAD(attrs,
                toStringValue(source, "source"),
                toStringValue(destination, "destination"),
                toLongValue(sequenceNumber, "sequence_number"),
                toStringValue(producerId, "producer_id"));
    }

    private static String toStringValue(Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        // Be permissive: coerce to string to avoid deserialization issues when types differ.
        return String.valueOf(value);
    }

    private static Long toLongValue(Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
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

    public Optional<Long> sequenceNumber() {
        return Optional.ofNullable(sequenceNumber);
    }

    public Optional<String> producerId() {
        return Optional.ofNullable(producerId);
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

    public void requireSequenceNumber(long expectedSequenceNumber) {
        if (sequenceNumber == null) {
            throw new IllegalArgumentException("AAD missing sequence number");
        }
        if (!sequenceNumber.equals(expectedSequenceNumber)) {
            throw new IllegalArgumentException("AAD sequence number mismatch: expected "
                    + expectedSequenceNumber + ", got " + sequenceNumber);
        }
    }

    @Override
    public String toString() {
        return "DecodedAAD{" +
                "attributes=" + attributes +
                ", sourceHash='" + sourceName + '\'' +
                ", destinationHash='" + destinationName + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", producerId='" + producerId + '\'' +
                '}';
    }
}
