package ch.usi.inf.examples.confidential_word_count.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public record SplitSentenceRequest(EncryptedValue jokeEntry) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SplitSentenceRequest {
        Objects.requireNonNull(jokeEntry, "Encrypted body cannot be null");
    }
}
