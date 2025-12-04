package ch.usi.inf.examples.confidential_word_count.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public record WordCountRequest(EncryptedValue word) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public WordCountRequest {
        Objects.requireNonNull(word, "Encrypted word cannot be null");
    }
}
