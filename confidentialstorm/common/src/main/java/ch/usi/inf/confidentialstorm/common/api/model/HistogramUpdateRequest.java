package ch.usi.inf.confidentialstorm.common.api.model;

import ch.usi.inf.confidentialstorm.common.api.model.base.IServiceMessage;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.util.Objects;

public record HistogramUpdateRequest(EncryptedValue word, EncryptedValue count) implements IServiceMessage {
    @Serial
    private static final long serialVersionUID = 1L;

    public HistogramUpdateRequest {
        Objects.requireNonNull(word, "Encrypted word cannot be null");
        Objects.requireNonNull(count, "Encrypted count cannot be null");
    }
}
