package ch.usi.inf.confidentialstorm.common.api.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

public record SplitSentenceRequest(EncryptedValue body) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SplitSentenceRequest {
        Objects.requireNonNull(body, "Encrypted body cannot be null");
    }
}
