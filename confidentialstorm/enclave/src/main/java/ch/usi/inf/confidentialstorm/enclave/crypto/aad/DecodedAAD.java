package ch.usi.inf.confidentialstorm.enclave.crypto.aad;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.util.AADUtils;

import java.util.*;

/**
 * Represents a decoded Additional Authenticated Data (AAD) payload.
 * Provides methods to access common AAD fields and verify routing.
 */
public final class DecodedAAD {
    private final Map<String, Object> attributes;
    private final String sourceName;
    private final String destinationName;
    private final Long sequenceNumber;
    private final String producerId;
    private final Integer epoch;

    private DecodedAAD(Map<String, Object> attributes,
                       String sourceName,
                       String destinationName,
                       Long sequenceNumber,
                       String producerId,
                       Integer epoch
    ) {
        this.attributes = attributes;
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.sequenceNumber = sequenceNumber;
        this.producerId = producerId;
        this.epoch = epoch;
    }

    /**
     * Decodes a DecodedAAD instance from its byte representation (JSON).
     *
     * @param aadBytes the AAD bytes to decode
     * @return the DecodedAAD instance
     */
    public static DecodedAAD fromBytes(byte[] aadBytes) {
        if (aadBytes == null || aadBytes.length == 0) {
            // empty
            return new DecodedAAD(Collections.emptyMap(), null, null, null, null, null);
        }
        Map<String, Object> parsed = AADUtils.parseAadJson(aadBytes);
        Object source = parsed.remove("source");
        Object destination = parsed.remove("destination");
        Object producerId = parsed.remove("producer_id");
        // optional: remove sequence number and epoch if present
        Object sequenceNumber = parsed.remove("seq");
        Object epoch = parsed.remove("epoch");
        Map<String, Object> attrs = Collections.unmodifiableMap(new LinkedHashMap<>(parsed));

        // construct DecodedAAD instance
        return new DecodedAAD(attrs,
                String.valueOf(source),
                String.valueOf(destination),
                toLongValue(sequenceNumber),
                String.valueOf(producerId),
                toIntegerValue(epoch));
    }

    private static Long toLongValue(Object value) {
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

    private static Integer toIntegerValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    /**
     * Gets the sequence number from the AAD.
     *
     * @return an Optional containing the sequence number, if present
     */
    public Optional<Long> sequenceNumber() {
        return Optional.ofNullable(sequenceNumber);
    }

    /**
     * Gets the producer ID from the AAD.
     *
     * @return an Optional containing the producer ID, if present
     */
    public Optional<String> producerId() {
        return Optional.ofNullable(producerId);
    }

    /**
     * Gets the epoch number from the AAD.
     *
     * @return an Optional containing the epoch number, if present
     */
    public Optional<Integer> epoch() {
        return Optional.ofNullable(epoch);
    }

    /**
     * Gets the source component name from the AAD.
     *
     * @return an Optional containing the source name, if present
     */
    public Optional<String> sourceName() {
        return Optional.ofNullable(sourceName);
    }

    /**
     * Gets the destination component name from the AAD.
     *
     * @return an Optional containing the destination name, if present
     */
    public Optional<String> destinationName() {
        return Optional.ofNullable(destinationName);
    }

    /**
     * Checks if the source in the AAD matches the given component.
     *
     * @param component the component to match against
     * @return true if matches, false otherwise
     */
    public boolean matchesSource(TopologySpecification.Component component) {
        Objects.requireNonNull(component, "Component cannot be null");
        if (sourceName == null) {
            return false;
        }
        return sourceName.equals(component.getName());
    }

    /**
     * Checks if the destination in the AAD matches the given component.
     *
     * @param component the component to match against
     * @return true if matches, false otherwise
     */
    public boolean matchesDestination(TopologySpecification.Component component) {
        Objects.requireNonNull(component, "Component cannot be null");
        if (destinationName == null) {
            return false;
        }
        return destinationName.equals(component.getName());
    }

    @Override
    public String toString() {
        return "DecodedAAD{" +
                "attributes=" + attributes +
                ", sourceName='" + sourceName + '\'' +
                ", destinationName='" + destinationName + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", producerId='" + producerId + '\'' +
                ", epoch=" + epoch +
                '}';
    }
}
