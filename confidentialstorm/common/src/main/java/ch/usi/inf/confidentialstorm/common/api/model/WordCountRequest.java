package ch.usi.inf.confidentialstorm.common.api.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import ch.usi.inf.confidentialstorm.common.api.model.base.IServiceMessage;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

public record WordCountRequest(String routingKey, EncryptedValue word) implements IServiceMessage {
    @Serial
    private static final long serialVersionUID = 1L;

    public WordCountRequest {
        if (routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("Routing key cannot be null or blank");
        }
        Objects.requireNonNull(word, "Encrypted word cannot be null");
    }
}
