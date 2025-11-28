package ch.usi.inf.examples.confidential_word_count.common.api.model;

import ch.usi.inf.confidentialstorm.common.api.IServiceMessage;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.util.Objects;

public record SplitSentenceRequest(EncryptedValue body) implements IServiceMessage {
    @Serial
    private static final long serialVersionUID = 1L;

    public SplitSentenceRequest {
        Objects.requireNonNull(body, "Encrypted body cannot be null");
    }
}
