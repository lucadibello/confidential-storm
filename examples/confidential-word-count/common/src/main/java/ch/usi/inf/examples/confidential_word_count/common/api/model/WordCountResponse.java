package ch.usi.inf.examples.confidential_word_count.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public record WordCountResponse(String routingKey, EncryptedValue word, EncryptedValue count)
        implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public WordCountResponse {
        if (routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("Routing key cannot be null or blank");
        }
        Objects.requireNonNull(word, "Encrypted word cannot be null");
        Objects.requireNonNull(count, "Encrypted count cannot be null");
    }
}
