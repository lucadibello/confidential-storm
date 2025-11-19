package ch.usi.inf.confidentialstorm.common.crypto.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Encrypted word plus a deterministic routing key derived from the plaintext.
 */
public record EncryptedWord(String routingKey, EncryptedValue payload) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public EncryptedWord {
        if (routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("Routing key cannot be null or blank");
        }
        Objects.requireNonNull(payload, "Payload cannot be null");
    }
}

