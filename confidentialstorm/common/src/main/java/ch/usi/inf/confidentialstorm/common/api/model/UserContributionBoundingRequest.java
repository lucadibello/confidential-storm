package ch.usi.inf.confidentialstorm.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public record UserContributionBoundingRequest(EncryptedValue word) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public UserContributionBoundingRequest {
        Objects.requireNonNull(word, "Word cannot be null");
    }
}
